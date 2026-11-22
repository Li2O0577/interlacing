"""Run against an installed wheel, from a directory that is not the source tree: the package imports, its
public names resolve, a graph runs, and the examples run.

    python scripts/package_smoke.py examples/graphs
"""
import json
import os
import runpy
import subprocess
import sys
import tempfile
from pathlib import Path

import interlacing
from interlacing import DEFAULT, Chain, Engine, FunctionNode, Port

source = Path(__file__).resolve().parents[1] / "src"
assert source not in Path(interlacing.__file__).resolve().parents, "the source tree is being imported, not the wheel"
for name in interlacing.__all__:
    getattr(interlacing, name)

node = lambda id, f: FunctionNode(id, f, inputs=[Port("x")], outputs=[Port("y")])  # noqa: E731
assert Engine(Chain(node("a", lambda x: x + 1), node("b", lambda x: x * 2)).build(DEFAULT), DEFAULT).run({"in": 1}).last("out") == 4

examples = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(examples))
for example in sorted(examples.glob("*.py")):
    runpy.run_path(str(example), run_name="__main__")

# The installed command: a graph written as data waits for a person, is answered, and carries on.
data = examples.parent / "data"
command = [str(Path(sys.executable).with_name("interlacing"))]
with tempfile.TemporaryDirectory() as temporary:
    where = ["--dir", str(Path(temporary) / "run")]

    def step(*argv, expect):
        done = subprocess.run(command + list(argv), capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(data)})
        assert done.returncode == expect, (argv, done.returncode, done.stdout, done.stderr)
        return json.loads(done.stdout)

    assert step("run", str(data / "release.json"), *where, "--allow", "steps", "--input", "topic=tides", expect=3)["status"] == "waiting"
    step("answer", *where, "--json", '{"approved": true}', expect=0)
    assert step("resume", *where, expect=0)["outputs"] == {"result": ["published: A note about tides."]}

    # Defaults, a prompt taken from a file, and a pool of ten places of which three are used.
    scouts = step("run", str(data / "scouts.json"), "--dir", str(Path(temporary) / "scouts"), "--allow", "steps",
                  "--input", 'in=["north", "east", "south"]', "--time-limit", "60", expect=0)
    assert sorted(scouts["outputs"]["out"][0]) == ["east", "north", "south"], scouts
    assert sum(1 for id in scouts["rounds"] if id.startswith("scout-") and id != "scout-roster") == 3, scouts

    # The server: the same graph through the Model Context Protocol, over stdio.
    ask = lambda id, method, **params: json.dumps({"jsonrpc": "2.0", "id": id, "method": method, "params": params})  # noqa: E731
    sent = "\n".join([ask(1, "initialize", protocolVersion="2025-06-18", capabilities={}), ask(2, "tools/list"),
                      ask(3, "tools/call", name="start", arguments={"graph": json.loads((data / "release.json").read_text()), "inputs": {"topic": "tides"}})])
    served = subprocess.run(command + ["serve", "--runs", str(Path(temporary) / "served"), "--allow", "steps"], input=sent + "\n",
                            capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(data)})
    replies = [json.loads(line) for line in served.stdout.splitlines()]
    assert served.returncode == 0 and len(replies[1]["result"]["tools"]) == 9, served.stderr
    assert {r["id"]: r for r in replies}[3]["result"]["structuredContent"]["status"] == "waiting", replies

print(f"interlacing {interlacing.__version__}: {len(interlacing.__all__) - 1} public names, {len(list(examples.glob('*.py')))} examples ran, the command line and the server ran")
