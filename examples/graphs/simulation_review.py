"""Two reviewers share one numerical experiment. Run this file directly for both outcomes."""
from interlacing import Engine, FunctionNode, GraphBuilder, Port


def experiment(wrong_units, calls, interrupt=None):
    def simulate(ctx, n):
        ctx.use(simulations=1)
        calls.append(n)
        # Forward Euler cooling with unit decay rate, starting at one.
        reading = (1 - 1 / n) ** n
        exported = reading * (100 if wrong_units else 1)
        return {"reading": exported, "percent": exported * 100}

    def assess(ctx, rejection):
        if interrupt:
            interrupt()
        if not wrong_units:
            return "The stored reading is a fraction; the other channel is a percentage."
        ctx.control("b2", "pause")
        fraction = rejection["content"] / 100
        for message in rejection["sent"]["percent"]:
            # Both exports used the same mistaken scale. Keep the percent port in percent.
            ctx.correct(message, fraction * 100)
        ctx.control("b2", "resume")
        ctx.correct(rejection["message"], fraction)

    def review(ctx, reading):
        answer = ctx.reject("reading", "Confirm the units of the two channels")
        return answer["answer"]

    def interpret(percent, verdict):
        if verdict not in ("explained", "corrected"):
            raise ValueError("review still unresolved")
        return percent / 100

    a = FunctionNode("a", simulate, inputs=[Port("n")], outputs=[Port("reading"), Port("percent")], context=True)
    a.challenged = assess
    b1 = FunctionNode("b1", review, inputs=[Port("reading")], outputs=[Port("verdict")], context=True)
    b2 = FunctionNode("b2", interpret, inputs=[Port("percent"), Port("verdict")], outputs=[Port("fraction")])
    report = FunctionNode("report", lambda fraction: round(fraction, 4),
                          inputs=[Port("fraction")], outputs=[Port("value")])
    return (GraphBuilder().add(a, b1, b2, report).manage("a", "b2")
            .connect("a.reading", "b1.reading", challenges=2)
            .connect("a.percent", "b2.percent").connect("b1.verdict", "b2.verdict")
            .connect("b2.fraction", "report.fraction").input("steps", "a.n")
            .output("report", "report.value").output("verdict", "b1.verdict").compile())


def main():
    for wrong_units in (False, True):
        calls = []
        result = Engine(experiment(wrong_units, calls)).run({"steps": 100})
        print(result.last("verdict"), result.last("report"), "simulation calls:", len(calls))
        assert len(calls) == 1 and result.last("report") == 0.366


if __name__ == "__main__":
    main()
