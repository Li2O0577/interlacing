"""The bilingual handbooks describe and exercise the installed package."""
import json
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import interlacing

ROOT = Path(__file__).resolve().parents[1]


def test_handbook_examples_and_api(tmp_path):
    editions = []
    for language in ('en', 'zh-CN'):
        text = (ROOT / 'docs' / f'HANDBOOK.{language}.md').read_text(encoding='utf-8')
        rows = re.findall(r'^\| `(\w+)` \| ((?:`\w+`(?:, )?)+) \|$', text, re.M)
        documented = {name: module for module, names in rows for name in re.findall(r'`(\w+)`', names)}
        assert documented == interlacing._PUBLIC, language
        editions.append(re.findall(r'^```(python|sh)\n(.*?)\n```$', text, re.M | re.S))
    assert editions[0] == editions[1]
    shutil.copytree(ROOT / 'examples', tmp_path / 'examples')
    for language, source in editions[0]:
        if language == 'python':
            subprocess.run([sys.executable, '-c', source], check=True, cwd=tmp_path)
            continue
        cwd = tmp_path
        for line in source.splitlines():
            args = shlex.split(line)
            if args[:4] == ['python', '-m', 'pip', 'install']:
                assert args[4:] == [f'interlacing=={interlacing.__version__}']
            elif args[0] == 'cd':
                cwd = cwd / args[1]
            elif args[0] == 'python':
                subprocess.run([sys.executable, *args[1:]], cwd=cwd, check=True)
            else:
                assert args[0] == 'interlacing'
                result = subprocess.run([sys.executable, '-m', 'interlacing.cli', *args[1:]], cwd=cwd,
                                        capture_output=True, text=True)
                assert result.returncode == (3 if args[1] == 'run' else 0), result.stdout
                if args[1] == 'resume':
                    assert json.loads(result.stdout)['outputs']['result'] == ['published: A note about tides.']
