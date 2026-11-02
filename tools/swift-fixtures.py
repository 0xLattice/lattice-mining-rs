#!/usr/bin/env python3
"""Actual pinned Swift miner core/coordinator, unchanged source, fake node/worker."""
import hashlib,json,pathlib,subprocess
root=pathlib.Path(__file__).resolve().parents[1];ref=root.parent/'lattice-node';cache=pathlib.Path('/tmp/lattice-genesis-oracle');pin='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ref,text=True).strip()==pin
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ref)
lattice=root.parent/'Lattice'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=lattice,text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
for original in (lattice/'Sources').rglob('*.swift'):
 assert original.read_bytes()==(cache/original.relative_to(lattice)).read_bytes()
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
sources={}
for target,files in [('LatticeMinerCore',['MinerLoopLogic.swift','ProofOfWork.swift']),('LatticeMiningCoordinator',['MiningCoordinator.swift'])]:
 out=cache/'Sources'/target;out.mkdir(exist_ok=True)
 for name in files:
  source=ref/'Sources'/target/name;(out/name).write_bytes(source.read_bytes());sources[str(source.relative_to(ref))]=hashlib.sha256(source.read_bytes()).hexdigest()
path=cache/'Sources/CoordinatorOracle';path.mkdir(exist_ok=True);(path/'Oracle.swift').write_bytes((root/'tests/coordinator-oracle.swift').read_bytes())
p=cache/'Package.swift';s=p.read_text()
if 'name: "CoordinatorOracle"' not in s:
 s=s.replace('    targets: [\n','''    targets: [
        .target(name: "LatticeMinerCore", dependencies: ["Lattice", .product(name: "UInt256", package: "UInt256"), .product(name: "Crypto", package: "swift-crypto")]),
        .target(name: "LatticeMiningCoordinator", dependencies: ["Lattice", "LatticeMinerCore", .product(name: "UInt256", package: "UInt256")]),
        .executableTarget(name: "CoordinatorOracle", dependencies: ["Lattice", "LatticeMinerCore", "LatticeMiningCoordinator", .product(name: "UInt256", package: "UInt256")]),
''',1);p.write_text(s)
raw=subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','CoordinatorOracle']);fixture=json.loads(raw)
file=root/'tests/swift-vectors.json';file.write_text(json.dumps(fixture,indent=2,sort_keys=True)+'\n')
report={'node_revision':pin,'lattice_revision':'dda2d852429a27be6043599b63bde81530cd9514','source_sha256':sources,'package_resolved_sha256':hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest(),'fixture_sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'scope':'Actual unchanged miner core/coordinator with deterministic injected worker/node; no live HTTP/mining.'}
(root/'tests/swift-evidence.json').write_text(json.dumps(report,indent=2)+'\n');print({k:len(v) for k,v in fixture.items()})
