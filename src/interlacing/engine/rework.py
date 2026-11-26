"""The durable index of messages, actual readers and pending rework."""
from __future__ import annotations


class Superseded(Exception):
    """The activation's bound inputs changed before it could publish."""


class Rework:
    def __init__(self, data: dict):
        self.messages: dict = data["messages"]
        self.deliveries: dict = data["deliveries"]
        self.readings: dict = data["readings"]
        self.readers: dict = data["readers"]
        self.waiting: dict = data["waiting"]

    def bind(self, record: dict) -> None:
        if "redo" in record:
            return
        node, round, took = record["node"], str(record["round"]), record["took"]
        self.readings.setdefault(node, {})[round] = {"took": list(took), "emits": {}, "replayable": True}
        for id in took:
            self.readers[id] = [node, round]

    def store(self, packed: list) -> str:
        id = f"{packed[0]}#{packed[1]}"
        self.deliveries[id] = packed
        return id

    def reading(self, node: str, pending: dict) -> dict:
        return self.readings[node][pending.get("redo", str(pending["round"]))]

    def current(self, node: str, pending: dict) -> bool:
        return self.reading(node, pending)["took"] == pending.get("effective", pending["took"]) or pending["resumed"]

    def emit(self, record: dict, pending: dict, position: int) -> None:
        node, token, port = record["node"], record["token"], record["port"]
        self.messages.setdefault(node, {})[token] = {
            "position": position, "round": record["round"], "port": port, "key": record["key"], "version": 1,
            "envelopes": {item[0]: f"{item[0]}#{item[1]}" for item in record["sent"]},
            "outputs": [[name, index] for name, index, _ in record["outputs"]],
        }
        self.reading(node, pending)["emits"].setdefault(port, []).append(token)

    def origin(self, node, edge, delivery, limit):
        messages = self.messages.get(node, {})
        for token, item in reversed(messages.items()):
            if item["envelopes"].get(edge) == delivery:
                reading = self.readings[node].get(str(item.get("round")), {})
                version = item.get("edge_versions", {}).get(edge, 1)
                if version != item["version"]:
                    return None
                others = [name for names in reading.get("emits", {}).values() for name in names if name != token]
                others.sort(key=lambda name: messages[name].get("position", 0))
                siblings: dict[str, list] = {}
                for name in others[-limit:]:
                    siblings.setdefault(messages[name]["port"], []).append(name)
                return token, version, siblings
        return None

    def revise(self, record: dict) -> None:
        message = self.messages[record["node"]][record["token"]]
        message.update(key=record["key"], version=record["version"])
        for packed, route in zip(record["sent"], record["routes"]):
            old = packed[5]
            new = self.store(packed)
            message["envelopes"][packed[0]] = new
            message.setdefault("edge_versions", {})[packed[0]] = record["version"]
            if route in ("rerun", "answer"):
                node, round = self.readers.pop(old)
                self.readers[new] = [node, round]
                reading = self.readings[node][round]
                reading["took"] = [new if id == old else id for id in reading["took"]]
                if route == "rerun":
                    self.waiting.setdefault(node, {})[round] = None

    def settle(self, record: dict, pending: dict) -> None:
        node = record["node"]
        if record.get("open"):
            self.reading(node, pending)["replayable"] = False
        elif "redo" in pending and not record.get("stale"):
            del self.waiting[node][pending["redo"]]
            if not self.waiting[node]:
                del self.waiting[node]

    def jobs(self) -> list[dict]:
        return [{"node": node, "round": int(round), "took": self.readings[node][round]["took"]}
                for node, rounds in self.waiting.items() for round in rounds]
