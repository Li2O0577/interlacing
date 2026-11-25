"""Fan-out timings and burst memory, using the package selected by PYTHONPATH."""
import statistics
import time
import tracemalloc

from interlacing import Engine, FunctionNode, GraphBuilder, Port


def graph(width, emit, consume):
    source = FunctionNode("source", emit, inputs=[Port("x")], outputs=[Port("out")], context=True)
    builder = GraphBuilder().add(source).input("in", "source.x")
    for index in range(width):
        target = FunctionNode(f"sink{index}", consume, inputs=[Port("x")])
        builder.add(target).connect("source.out", (target.id, "x"))
    return builder.compile()


def timing(width, size):
    def emit(ctx, x):
        for _ in range(128):
            ctx.emit("out", x)

    plan = graph(width, emit, lambda x: None)
    inputs = {"in": "x" * size}
    samples = []
    for _ in range(5):
        engine = Engine(plan)
        start = time.perf_counter()
        result = engine.run(inputs)
        samples.append(time.perf_counter() - start)
        assert result.envelopes == 1 + width * 128
    return 1000 * statistics.median(samples)


def burst(count):
    produced = consumed = peak_pending = 0

    def emit(ctx, x):
        nonlocal produced, peak_pending
        for index in range(x):
            ctx.emit("out", str(index).zfill(8192))
            produced += 1
            peak_pending = max(peak_pending, produced - consumed)

    def consume(x):
        nonlocal consumed
        consumed += 1
        time.sleep(0.0001)

    # One worker: the whole burst is queued before the first consumer activation.
    engine = Engine(graph(1, emit, consume))
    tracemalloc.start()
    try:
        engine.run({"in": count})
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert consumed == count
    return peak_pending, retained / 2**20, peak / 2**20


if __name__ == "__main__":
    for size in (16, 65536):
        for width in (1, 8):
            print(f"{width} sinks, {size} bytes: {timing(width, size):.2f} ms")
    for count in (128, 2048):
        pending, retained, peak = burst(count)
        print(f"{pending} pending, 8 KiB each: {peak:.2f} MiB peak, {retained:.2f} MiB after drain")
