"""Committed challenge identities and replies, local to one durable graph scope."""
from .journal import unpack_body
from .wiring import Rejection


class DurableReviews:
    def __init__(self, records, counts, deliveries):
        self.records = records
        self.counts = counts
        self.deliveries = deliveries
        self.live: dict[str, Rejection] = {}
        self.unanswered: dict[str, str] = {}

    def opened(self, event, position):
        self.counts[event["edge"]] = event["number"]
        self.records[event["id"]] = {**event, "position": position}
        self.unanswered[event["envelope"]] = event["id"]

    def settled(self, result):
        item = self.records[result["id"]]
        item["result"] = result
        self.unanswered.pop(item["envelope"], None)
        question = self.live.get(item["id"])
        if question is not None:
            self.apply_result(question, result)
            source = question.feed.source.reviews
            if question in source.pending:
                source.pending.remove(question)
            source.open.pop(question.id, None)

    def content(self, feed, delivery):
        # The journal already holds the wire value (a Ref for artifacts, including binary assets).
        body = unpack_body(self.deliveries[delivery][3])
        return feed.protocol.decode(body, feed.port.port)

    def apply_result(self, question, result):
        question.settle(result["answer"], result.get("text", ""),
                        self.content(question.feed, result["delivery"]) if "delivery" in result else None)

    @staticmethod
    def references(binding):
        if binding is None:
            return set()
        ids = set(binding.get("review_open", ()))
        if "answering" in binding:
            ids.add(binding["answering"])
        for op in binding["ops"]:
            if op["kind"] == "review_take":
                ids.update(op["ids"])
            elif op["kind"] in ("challenge", "review_reply") and "id" in op:
                ids.add(op["id"])
        return ids

    def release(self, candidates, bindings):
        # The immutable events retain the audit trail. Keep a verdict in the checkpoint
        # only while a stopped receiver or answering activation could still replay it.
        for id in candidates:
            item = self.records.get(id)
            if item is None or "result" not in item:
                continue
            if any(id in self.references(bindings.get(node)) for node in (item["node"], item["sender"])):
                continue
            del self.records[id]
            self.live.pop(id, None)

    def rebuild(self, engine, bindings):
        for edge, count in self.counts.items():
            engine._feeds[edge].rejected = count
        # PostgreSQL JSONB does not preserve the order of object keys.
        assigned = {p["answering"] for p in bindings.values() if "answering" in p}
        held = {id for pending in bindings.values() for id in pending.get("review_open", ())}
        assigned.update(held)
        for pending in bindings.values():
            for op in pending["ops"]:
                if op["kind"] == "review_take":
                    assigned.update(op["ids"])
        for item in sorted(self.records.values(), key=lambda item: item["position"]):
            feed = engine._feeds[item["edge"]]
            question = Rejection(item["id"], feed, item["envelope"], feed.out, item["message"],
                                 self.content(feed, item["envelope"]), item["reason"], item["node"], item["siblings"])
            self.live[question.id] = question
            if "result" in item:
                self.apply_result(question, item["result"])
            else:
                self.unanswered[question.envelope] = question.id
                if question.id in held:
                    feed.source.reviews.open[question.id] = question
                elif question.id not in assigned:
                    feed.source.reviews.pending.append(question)

    @staticmethod
    def describe(question, state, reason, number, version):
        return {"kind": "challenge", "node": state.node.id, "round": state.round, "id": question.id,
                "number": number, "version": version, "edge": question.feed.id, "envelope": question.envelope,
                "sender": question.feed.source.node.id, "message": question.message, "siblings": question.sent,
                "reason": reason}
