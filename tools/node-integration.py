#!/usr/bin/env python3
"""Real local Rust node/coordinator/external CPU worker integration, no public peers.

Build sibling binaries first. Timing gates delay delivery of a genuine CPU result
only in stale/cancellation cases; successful mining uses the external worker directly.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def request(base, route, cookie, body=None):
    headers = {'Content-Type': 'application/json', 'Authorization': 'Basic ' + base64.b64encode(cookie.encode()).decode()}
    req = urllib.request.Request(base + route, data=None if body is None else json.dumps(body).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def start_node(binary, config, logs):
    child = subprocess.Popen([str(binary), '--config', str(config)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ready = queue.Queue()
    def drain(pipe, selected):
        for line in pipe:
            logs.append(line.rstrip())
            if selected and line.startswith('{'):
                ready.put(json.loads(line))
    threading.Thread(target=drain, args=(child.stdout, True), daemon=True).start()
    threading.Thread(target=drain, args=(child.stderr, False), daemon=True).start()
    try:
        return child, ready.get(timeout=30)
    except Exception:
        child.kill(); child.wait()
        raise RuntimeError('node failed to start: ' + '\n'.join(logs[-10:]))


def stop(child):
    child.terminate()
    assert child.wait(timeout=15) == 0, 'node did not stop cleanly'


def cycle(child):
    out, err = child.communicate(timeout=20)
    lines = [json.loads(line) for line in out.splitlines() if line.startswith('{')]
    assert len(lines) == 1, (out, err)
    return child.returncode, lines[0]


def wait_file(path, child):
    deadline = time.monotonic() + 15
    while not path.exists():
        assert child.poll() is None, 'coordinator exited before worker completed'
        assert time.monotonic() < deadline, 'worker did not produce its gated result'
        time.sleep(.02)
    return json.loads(path.read_text())


def wait_gone(pid):
    deadline = time.monotonic() + 5
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        assert time.monotonic() < deadline, f'worker process {pid} survived cancellation'
        time.sleep(.02)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'target/node-integration.json')
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {'passed': False, 'scope': 'disposable loopback-only node, coordinator and external CPU worker'}
    processes, logs = [], []
    fixture = ROOT.parent / 'lattice-rs/crates/lattice-validation/tests/genesis-swift.json'
    try:
        with tempfile.TemporaryDirectory(prefix='lattice-mining-integration-') as directory:
            tmp = Path(directory)
            binaries = tmp / 'bin'; binaries.mkdir()
            paths = [ROOT.parent / 'lattice-node-rs/target/debug/lattice-node', ROOT / 'target/debug/lattice-mining-coordinator', ROOT.parent / 'lattice-miner-gpu/target/debug/lattice-miner-gpu']
            node_bin, coordinator_bin, worker_bin = [Path(shutil.copy2(p, binaries / p.name)) for p in paths]
            report['binaries'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (node_bin, coordinator_bin, worker_bin)}
            report['genesis_fixture_sha256'] = hashlib.sha256(fixture.read_bytes()).hexdigest()
            report['harness_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            with (tmp / 'bootstrap.json').open('w') as output:
                subprocess.run(['cargo', 'run', '--quiet', '--locked', '-p', 'lattice-validation', '--example', 'genesis_volumes', '--', str(fixture)], cwd=ROOT.parent / 'lattice-rs', stdout=output, check=True)
            config = {'directory': 'data', 'identityKey': 'identity.key', 'genesisCid': json.loads(fixture.read_text())[0]['cid'], 'bootstrapFiles': ['bootstrap.json'], 'minRelayFee': 0, 'listen': '127.0.0.1:0', 'rpc': '127.0.0.1:0'}
            config_path = tmp / 'node.json'; config_path.write_text(json.dumps(config))
            node, ready = start_node(node_bin, config_path, logs); processes.append(node)
            base = 'http://' + ready['rpc']; cookie_file = tmp / 'data/.cookie'; cookie = cookie_file.read_text().strip()
            def status():
                code, result = request(base, '/status', cookie)
                assert code == 200, result
                return result
            def coordinator(executable=worker_bin, extra=(), cookie_path=cookie_file):
                command = [str(coordinator_bin), '--node', base, '--rpc-cookie-file', str(cookie_path), '--workers', '1', '--batch-size', '64', '--once', '--worker-executable', str(executable)]
                for argument in extra:
                    command += ['--worker-arg=' + str(argument)]
                child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                processes.append(child)
                return child
            before = status(); assert before['height'] == '0', before
            unauthorized = tmp / 'wrong.cookie'; unauthorized.write_text('invalid:cookie')
            child = coordinator(extra=['--backend', 'cpu'], cookie_path=unauthorized)
            code, refused = cycle(child); processes.remove(child)
            assert code != 0 and refused['result'] == 'nodeFailed', refused
            assert status()['height'] == '0'
            child = coordinator(extra=['--backend', 'cpu'])
            code, mined = cycle(child); processes.remove(child)
            assert code == 0 and mined['result'] == 'submitted' and mined['accepted'], mined
            accepted = status(); assert accepted['height'] == '1' and accepted['tipCID'] != before['tipCID'], accepted
            # Execute the real CPU worker on the unchanged node assignment, then
            # gate delivery so status changes and SIGINT have deterministic timing.
            wrapper = tmp / 'gated_worker.py'
            wrapper.write_text('''import json,os,pathlib,subprocess,sys,time
worker,marker=sys.argv[1:3]
result=subprocess.check_output([worker,'--backend','cpu']+sys.argv[3:])
value=json.loads(result)
assert value['status']=='found',value
path=pathlib.Path(marker)
temp=path.with_suffix('.tmp')
temp.write_text(json.dumps({'pid':os.getpid(),'workerResult':value}))
os.replace(temp,path)
while True:time.sleep(1)
''')
            marker = tmp / 'stale.json'
            child = coordinator(Path(sys.executable), [wrapper, worker_bin, marker])
            held = wait_file(marker, child)
            code, template = request(base, '/mining/templates', cookie, {'recipients': []}); assert code == 200, template
            code, advance = request(base, '/mining/work', cookie, {'workID': template['workID'], 'nonce': 0}); assert code == 200, advance
            assert status()['height'] == '2'
            code, stale = cycle(child); processes.remove(child)
            assert code == 0 and stale['result'] == 'stale', stale
            assert stale['workId'] == held['workerResult']['workId'], (stale, held)
            wait_gone(held['pid']); assert status()['height'] == '2'
            marker = tmp / 'cancel.json'
            child = coordinator(Path(sys.executable), [wrapper, worker_bin, marker])
            held = wait_file(marker, child)
            child.send_signal(signal.SIGINT)
            code, cancelled = cycle(child); processes.remove(child)
            assert code == 0 and cancelled['result'] == 'cancelled', cancelled
            wait_gone(held['pid']); assert status()['height'] == '2'
            stable = status(); stop(node); processes.remove(node)
            config['bootstrapFiles'] = []; config_path.write_text(json.dumps(config))
            node, ready = start_node(node_bin, config_path, logs); processes.append(node)
            base = 'http://' + ready['rpc']; cookie = cookie_file.read_text().strip()
            recovered = status(); assert recovered['height'] == stable['height'] and recovered['tipCID'] == stable['tipCID'], recovered
            child = coordinator(extra=['--backend', 'cpu'])
            code, resumed = cycle(child); processes.remove(child)
            assert code == 0 and resumed['result'] == 'submitted' and resumed['accepted'], resumed
            final = status(); assert final['height'] == '3', final
            stop(node); processes.remove(node)
            node, ready = start_node(node_bin, config_path, logs); processes.append(node)
            base = 'http://' + ready['rpc']; cookie = cookie_file.read_text().strip()
            durable = status(); assert durable['height'] == '3' and durable['tipCID'] == final['tipCID'], durable
            stop(node); processes.remove(node)
            report.update(passed=True, unauthorized=refused, initial_mining=mined, stale=stale, cancelled=cancelled, resumed_mining=resumed, durable_height=durable['height'], durable_tip=durable['tipCID'], gated_worker_teardown=True, timing_scope='stale/cancel cases run the real CPU worker then withhold its result; successful mining uses the worker directly')
    finally:
        for child in processes:
            child.kill(); child.wait()
        args.output.with_suffix('.log').write_text('\n'.join(logs) + '\n')
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
