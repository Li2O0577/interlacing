"""The engine awaited, and nodes whose functions are coroutines."""
import asyncio
import threading
import time

import pytest

from interlacing import DEFAULT, Chain, Engine, FunctionNode, Journal, Port, RunError


def fn(id, function, **options):
    return FunctionNode(id, function, inputs=[Port('x')], outputs=[Port('y')], **options)


def test_a_run_is_awaited_while_the_loop_stays_free():
    async def main():
        ticks = []

        async def tick():
            while True:
                ticks.append(1)
                await asyncio.sleep(0.005)
        ticking = asyncio.create_task(tick())
        graph = Chain(fn('slow', lambda x: (time.sleep(0.1), x + 1)[1]), fn('double', lambda x: x * 2)).build(DEFAULT)
        result = await Engine(graph, DEFAULT).arun({'in': 1})
        ticking.cancel()
        return result, len(ticks)
    result, ticks = asyncio.run(main())
    assert result.outputs == {'out': [4]} and ticks > 5


def test_several_runs_are_awaited_together():
    async def main():
        graphs = [Chain(fn('a', lambda x, i=i: x + i)).build(DEFAULT) for i in range(5)]
        return await asyncio.gather(*(Engine(graph, DEFAULT).arun({'in': 10}) for graph in graphs))
    assert [result.last('out') for result in asyncio.run(main())] == [10, 11, 12, 13, 14]


def test_a_node_function_may_be_a_coroutine():
    async def later(x):
        await asyncio.sleep(0.01)
        return x * 3

    async def with_context(ctx, x):
        await asyncio.sleep(0)
        ctx.emit('y', x)
        ctx.emit('y', x + 1)
    assert Engine(Chain(fn('later', later)).build(DEFAULT), DEFAULT).run({'in': 2}).outputs == {'out': [6]}
    assert Engine(Chain(fn('twice', with_context, context=True)).build(DEFAULT), DEFAULT).run({'in': 2}).outputs == {'out': [2, 3]}

    async def main():
        return await Engine(Chain(fn('later', later)).build(DEFAULT), DEFAULT, workers=2).arun({'in': 5})
    assert asyncio.run(main()).outputs == {'out': [15]}


def test_a_failure_reaches_the_one_who_awaits():
    async def broken(x):
        raise ValueError('no good')

    async def main():
        await Engine(Chain(fn('broken', broken)).build(DEFAULT), DEFAULT).arun({'in': 1})
    with pytest.raises(RunError, match='node broken failed in round 1: no good'):
        asyncio.run(main())


def test_cancelling_the_wait_stops_the_run():
    started, saw = threading.Event(), threading.Event()

    def waits(ctx, x):
        started.set()
        while not ctx.stopping():
            time.sleep(0.005)
        saw.set()

    async def main():
        engine = Engine(Chain(fn('waits', waits, context=True)).build(DEFAULT), DEFAULT)
        task = asyncio.create_task(engine.arun({'in': 1}))
        await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(main())
    assert saw.wait(5)


def test_a_run_is_resumed_and_gone_on_with_awaited(tmp_path):
    def fails_once(x, seen=[]):
        seen.append(x)
        if len(seen) == 1:
            raise ValueError('first time')
        return x + 1

    def build():
        return Chain(fn('first', lambda x: x * 2), fn('second', fails_once)).build(DEFAULT)

    async def main():
        with pytest.raises(RunError):
            await Engine(build(), DEFAULT, journal=Journal(tmp_path / 'run.jsonl')).arun({'in': 4})
        resumed = await Engine(build(), DEFAULT, journal=Journal(tmp_path / 'run.jsonl')).aresume()
        gone = await Engine(build(), DEFAULT, journal=Journal(tmp_path / 'other.jsonl')).ago({'in': 1})
        return resumed, gone
    resumed, gone = asyncio.run(main())
    assert resumed.outputs == {'out': [9]} and gone.outputs == {'out': [3]}


# ---- nodes that are coroutines, run on the engine's event loop -------------------------------------------

from interlacing import GraphBuilder  # noqa: E402


def fan_out(count, member, **options):
    """One source that sends its input to ``count`` copies of ``member``, each with its own output."""
    def source(x):
        return x
    builder = GraphBuilder().add(FunctionNode('source', source, inputs=[Port('x')], outputs=[Port('y')]))
    for i in range(count):
        builder.add(FunctionNode(f'm{i}', member, inputs=[Port('x')], outputs=[Port('y')], **options))
        builder.connect('source.y', f'm{i}.x').output(f'm{i}', f'm{i}.y')
    return builder.input('in', 'source.x').compile(DEFAULT)


def test_many_coroutine_nodes_wait_at_once_without_a_thread_each():
    async def waits(x):
        await asyncio.sleep(0.3)
        return x + 1
    graph = fan_out(100, waits)
    most = []

    def count(event):
        most.append(threading.active_count())
    before, started = threading.active_count(), time.monotonic()
    result = Engine(graph, DEFAULT, workers=100, observe=count).run({'in': 1})
    took = time.monotonic() - started
    assert all(result.outputs[f'm{i}'] == [2] for i in range(100))
    assert took < 3                              # one after another it would take 30 seconds
    assert max(most) <= before + 3      # the loop's thread and one for the source: none for the hundred


def test_with_no_workers_coroutine_nodes_run_one_at_a_time_in_a_fixed_order():
    order = []

    async def note(x):
        order.append(x)
        await asyncio.sleep(0)
        return x + 1

    def plain(x):
        order.append(x)
        return x + 1
    graph = Chain(fn('a', note), fn('b', plain), fn('c', note)).build(DEFAULT)
    assert Engine(graph, DEFAULT).run({'in': 1}).last('out') == 4 and order == [1, 2, 3]


def test_an_overrunning_coroutine_is_cancelled():
    cancelled = threading.Event()

    async def hangs(x):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise
    started = time.monotonic()
    with pytest.raises(RunError, match='ran longer than 0.2 seconds'):
        Engine(Chain(fn('hangs', hangs)).build(DEFAULT), DEFAULT, time_limit=0.2).run({'in': 1})
    assert cancelled.is_set() and time.monotonic() - started < 5


def test_a_coroutine_node_sees_the_run_stopping():
    saw = threading.Event()

    async def watches(ctx, x):
        while not ctx.stopping():
            await asyncio.sleep(0.01)
        saw.set()

    async def main():
        engine = Engine(Chain(fn('w', watches, context=True)).build(DEFAULT), DEFAULT)
        task = asyncio.create_task(engine.arun({'in': 1}))
        await asyncio.sleep(0.1)
        engine.stop()
        return await task
    result = asyncio.run(main())
    assert saw.is_set() and not result.complete


def test_cancelling_the_wait_cancels_coroutine_nodes():
    cancelled = threading.Event()

    async def hangs(x):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def main():
        task = asyncio.create_task(Engine(Chain(fn('h', hangs)).build(DEFAULT), DEFAULT).arun({'in': 1}))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(main())
    assert cancelled.is_set()


def test_a_coroutine_node_awaits_a_call():
    async def asks(ctx, x):
        return await ctx.acall('ask', x) * 10

    async def answers(x):
        await asyncio.sleep(0.01)
        return x + 1
    caller = FunctionNode('caller', asks, inputs=[Port('x')], outputs=[Port('y'), Port('ask', call=True)], context=True)
    graph = (GraphBuilder().add(caller, fn('answer', answers)).connect('caller.ask', 'answer.x', 'call')
             .input('in', 'caller.x').output('out', 'caller.y').compile(DEFAULT))
    result = Engine(graph, DEFAULT).run({'in': 4})
    assert result.last('out') == 50 and result.rounds == {'caller': 1, 'answer': 1}

    async def sync_call(ctx, x):
        return ctx.call('ask', x)
    caller = FunctionNode('caller', sync_call, inputs=[Port('x')], outputs=[Port('y'), Port('ask', call=True)], context=True)
    graph = (GraphBuilder().add(caller, fn('answer', answers)).connect('caller.ask', 'answer.x', 'call')
             .input('in', 'caller.x').output('out', 'caller.y').compile(DEFAULT))
    with pytest.raises(RunError, match='acall'):
        Engine(graph, DEFAULT).run({'in': 4})


def test_a_node_class_may_open_run_and_close_as_coroutines():
    from interlacing import Node
    seen = []

    class Held(Node):
        kind = 'held_for_test'

        async def open(self, ctx):
            seen.append('open')

        async def run(self, ctx, inputs):
            await asyncio.sleep(0)
            return {'y': inputs['x'] * 2}

        async def close(self, ctx):
            seen.append('close')
    node = Held('held', inputs=[Port('x')], outputs=[Port('y')])
    assert node.asynchronous
    assert Engine(Chain(node).build(DEFAULT), DEFAULT).run({'in': 3}).last('out') == 6
    assert seen == ['open', 'close']


def test_a_run_with_coroutine_nodes_is_taken_up_from_its_journal(tmp_path):
    tries = []

    async def once_fails(x):
        tries.append(x)
        if len(tries) == 1:
            raise ValueError('first time')
        return x + 1

    def build():
        return Chain(fn('first', lambda x: x * 2), fn('second', once_fails)).build(DEFAULT)
    with pytest.raises(RunError, match='first time'):
        Engine(build(), DEFAULT, journal=Journal(tmp_path / 'run.jsonl')).run({'in': 4})

    async def main():
        return await Engine(build(), DEFAULT, journal=Journal(tmp_path / 'run.jsonl')).aresume()
    result = asyncio.run(main())
    assert result.outputs == {'out': [9]} and tries == [8, 8]


def test_run_is_called_from_inside_a_running_loop():
    async def later(x):
        await asyncio.sleep(0)
        return x + 1

    async def main():
        return Engine(Chain(fn('later', later)).build(DEFAULT), DEFAULT).run({'in': 1})
    assert asyncio.run(main()).last('out') == 2


def test_an_inner_graph_is_awaited_on_the_same_loop():
    async def inner_node(x):
        await asyncio.sleep(0)
        return x * 3

    async def outer(ctx, x):
        inner = ctx.engine(Chain(fn('inner', inner_node)).build(DEFAULT))
        return (await inner.arun({'in': x})).last('out')
    graph = Chain(fn('outer', outer, context=True)).build(DEFAULT)
    assert Engine(graph, DEFAULT).run({'in': 2}).last('out') == 6


def test_shared_slots_cancel_nested_async_work_before_next_admission():
    from interlacing import GraphBuilder, GraphNode
    started = threading.Event()
    later = threading.Event()

    async def wait(ctx):
        started.set()
        while not ctx.stopping():
            await asyncio.sleep(0.01)

    async def untouched():
        later.set()

    child = GraphBuilder().add(FunctionNode('wait', wait, inputs=[], outputs=[], context=True),
                               FunctionNode('next', untouched, inputs=[], outputs=[])).compile(DEFAULT)
    root = GraphBuilder().add(GraphNode('group', child, workers=2)).compile(DEFAULT)
    engine = Engine(root, workers=1, shared_workers=True)

    async def main():
        task = asyncio.create_task(engine.arun())
        assert await asyncio.to_thread(started.wait, 3)
        engine.stop()
        result = await asyncio.wait_for(task, 3)
        assert not result.complete

    asyncio.run(main())
    assert not later.is_set()


def test_timed_out_child_returns_while_abandoned_thread_is_still_alive():
    from interlacing import GraphBuilder, GraphNode
    release = threading.Event()
    task = FunctionNode('slow', lambda: release.wait(3), inputs=[], outputs=[Port('answer')])
    task.time_limit = 0.06
    graph = GraphBuilder().add(GraphNode('group', GraphBuilder().add(task).compile(DEFAULT))).compile(DEFAULT)
    began = time.monotonic()
    try:
        with pytest.raises(RunError, match="longer than"):
            Engine(graph, workers=1, shared_workers=True).run()
        assert time.monotonic() - began < 2
    finally:
        release.set()
