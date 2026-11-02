import Foundation
import Lattice
import LatticeMinerCore
@testable import LatticeMiningCoordinator
import UInt256

enum Failure:Error {case fixture}
actor Ledger {var rows:[[String:String]]=[];func add(_ work:MiningCoordinatorWork,_ range:NonceSearchRange){rows.append(["workId":work.workId,"target":work.targetHex,"start":String(range.startNonce),"count":String(range.count)])}}
actor Node:MiningCoordinatorNodeClient {
 let mode:String;let work:MiningCoordinatorWork;var fetches=0;var submissions:[UInt64]=[]
 init(_ mode:String,_ work:MiningCoordinatorWork){self.mode=mode;self.work=work}
 func fetchWork()async throws->MiningCoordinatorWork? {fetches+=1;if mode=="no_work"{return nil};if mode=="offsets"&&fetches==3{return MiningCoordinatorWork(workId:"two",blockHex:"00",targetHex:work.targetHex,staleToken:"stable")};return work}
 func fetchStaleToken()async throws->String? {mode=="stale_retry" && !submissions.isEmpty ? "changed":"stable"}
 func submit(workId:String,nonce:UInt64)async throws->MiningSolutionSubmission {submissions.append(nonce);if mode=="unauthorized"{throw MiningCoordinatorNodeClientError.unauthorized(statusCode:401)};if mode=="exhaust_retries"||mode=="stale_retry"||(mode=="transient"&&submissions.count==1){throw Failure.fixture};if mode=="merged"&&submissions.count==1{return .init(accepted:false,disposition:"childOnly")};return .init(accepted:mode != "rejected",disposition:mode == "rejected" ? "refused":"accepted")}
}
func output(_ result:MiningCoordinatorCycleResult)->[String:String]{switch result{case .backoff:return ["result":"backoff"];case .nodeFailed:return ["result":"nodeFailed"];case .noSolution(let id):return ["result":"noSolution","workId":id];case .stale(let id):return ["result":"stale","workId":id];case .workerFailed(let id,let worker,_):return ["result":"workerFailed","workId":id,"workerId":worker];case .submitted(let id,let nonce,let submission):return ["result":"submitted","workId":id,"nonce":String(nonce),"accepted":String(submission.accepted),"disposition":submission.disposition]}}
@main struct Oracle {static func main()async throws {
 var ranges:[[String:Any]]=[]
 for total:UInt64 in [0,1,2,5,100,UInt64.max] {for workers in [0,1,2,3,7,32] {for offset:UInt64 in [0,UInt64.max-2] {ranges.append(["total":String(total),"workers":workers,"offset":String(offset),"ranges":MiningCoordinator.allocateRanges(totalBatchSize:total,workerCount:workers,nonceOffset:offset).map{["start":String($0.startNonce),"count":String($0.count)]}])}}}
 var parses:[[String:Any]]=[]
 for value in ["","0","1","0x1","f","0xF","0XFF","-1"," ",String(repeating:"f",count:64),String(repeating:"f",count:65),"2^0","2^32","2^255","2^256","2^-1","01","18446744073709551616"] {parses.append(["input":value,"target":MinerLoopLogic.parseTarget(value).map{$0.toHexString()} as Any? ?? NSNull(),"work":MinerLoopLogic.parseMinimumWork(value).map{$0.toHexString()} as Any? ?? NSNull()])}
 var cycles:[[String:Any]]=[]
 for mode in ["accepted","rejected","transient","exhaust_retries","unauthorized","stale_retry","no_work","worker_failure","no_solution","wrong_id","offsets","merged","expire"] {
 let ledger=Ledger();let prefix=ContiguousArray("fixture".utf8);let midstate=ProofOfWork.midstate(prefixBytes:prefix);let first=ProofOfWork.hash(midstate:midstate,nonce:0);var hard=first;var last:UInt64=1
 while true {hard=ProofOfWork.hash(midstate:midstate,nonce:last);if hard<first{break};last+=1}
 let work=MiningCoordinatorWork(workId:"one",blockHex:"00",prefixHex:mode=="merged" ? Data(prefix).map{String(format:"%02x",$0)}.joined():"",targetHex:String(repeating:"f",count:64),targets:mode=="merged" ? [.max,hard]:[.max],expiresInMilliseconds:mode=="expire" ? 1:nil,staleToken:"stable")
 let node=Node(mode,work)
 let worker=MiningCoordinatorWorker(id:"test"){work,range in
 await ledger.add(work,range)
 if mode=="expire" {try await Task.sleep(nanoseconds:50_000_000);return nil}
 if mode=="worker_failure"{throw Failure.fixture};if mode=="no_solution"||mode=="offsets"{return nil}
 let nonce:UInt64
 if mode=="merged" {guard let found=ProofOfWork.searchBatch(midstate:midstate,target:MinerLoopLogic.parseTarget(work.targetHex)!,startNonce:range.startNonce,count:range.count)else{return nil};nonce=found}else{nonce=range.startNonce}
 return .init(workerId:"test",workId:mode=="wrong_id" ? "wrong":work.workId,nonce:nonce)
 }
 let coordinator=MiningCoordinator(nodeClient:node,workers:[worker],totalBatchSize:mode=="merged" ? last+1:8,nonceOffset:mode=="offsets" ? UInt64.max-5:0,staleProbeEnabled:false,retryBackoffDelay:.zero)
 var results:[[String:String]]=[];for _ in 0..<(mode=="offsets" ? 3:1){results.append(output(await coordinator.runBatch()))}
 let m=await coordinator.metrics();cycles.append(["mode":mode,"results":results,"assignments":await ledger.rows,"submissions":await node.submissions.map(String.init),"metrics":["assigned":m.assignedRangeCount,"stale":m.staleAbortCount,"failure":m.workerFailureCount,"accepted":m.acceptedSolutionCount,"rejected":m.rejectedSolutionCount,"backoff":m.retryBackoffCount]])
 }
 let data=try JSONSerialization.data(withJSONObject:["ranges":ranges,"parses":parses,"cycles":cycles],options:[.sortedKeys]);print(String(decoding:data,as:UTF8.self))
}}
