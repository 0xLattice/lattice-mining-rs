use crate::*;
use futures_util::{FutureExt, future::BoxFuture};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};
use tokio_util::sync::CancellationToken;
pub struct CpuWorker {
    id: String,
}
impl CpuWorker {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}
impl Worker for CpuWorker {
    fn id(&self) -> &str {
        &self.id
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        let id = self.id.clone();
        async move {
            let bytes = hex::decode(&work.block_hex).map_err(|e| Error::Worker(e.to_string()))?;
            let block: lattice_primitives::Block = lattice_cashew::cbor::decode(&bytes)?;
            let prefix = block.pow_prefix();
            tokio::task::spawn_blocking(move || {
                let mut base = Sha256::new();
                base.update(prefix);
                for offset in 0..range.count {
                    let nonce = range.start_nonce.wrapping_add(offset);
                    if offset & 1023 == 0 && cancel.is_cancelled() {
                        return None;
                    }
                    let mut h = base.clone();
                    h.update(nonce.to_be_bytes());
                    let hash: [u8; 32] = h.finalize().into();
                    if hash <= work.target {
                        return Some(WorkerResult {
                            worker_id: id,
                            work_id: work.work_id.clone(),
                            nonce,
                        });
                    }
                }
                None
            })
            .await
            .map_err(|e| Error::Worker(e.to_string()))
        }
        .boxed()
    }
}
#[derive(Clone)]
pub struct ProcessWorker {
    id: String,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub maximum_stdout: usize,
    pub drain_timeout: Duration,
}
impl ProcessWorker {
    pub fn new(id: impl Into<String>, executable: PathBuf) -> Self {
        Self {
            id: id.into(),
            executable,
            arguments: vec![],
            maximum_stdout: 64 * 1024,
            drain_timeout: Duration::from_secs(1),
        }
    }
}
#[cfg(unix)]
struct GroupGuard(i32);
#[cfg(unix)]
impl Drop for GroupGuard {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
struct ReaderGuard<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ReaderGuard<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
    tail: bool,
) -> Result<Vec<u8>, Error> {
    let mut out = vec![];
    let mut chunk = [0; 8192];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok(out);
        }
        if tail {
            out.extend_from_slice(&chunk[..n]);
            if out.len() > limit {
                out.drain(..out.len() - limit);
            }
        } else {
            if n > limit.saturating_sub(out.len()) {
                return Err(Error::Worker("stdout exceeds byte limit".into()));
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }
}
impl Worker for ProcessWorker {
    fn id(&self) -> &str {
        &self.id
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move{
  let mut command=Command::new(&self.executable);command.args(&self.arguments).args(["--work-id",&work.work_id,"--block-hex",&work.block_hex,"--target",&hex::encode(work.target),"--start-nonce",&range.start_nonce.to_string(),"--count",&range.count.to_string()]);
  if !work.prefix.is_empty(){command.args(["--prefix-hex",&hex::encode(&work.prefix)]);}
  command.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).kill_on_drop(true);
  #[cfg(unix)]{use std::os::unix::process::CommandExt;command.as_std_mut().process_group(0);}
  let mut child=command.spawn()?;
  #[cfg(unix)]let group=GroupGuard(child.id().expect("spawned PID") as i32);
  let stdout=child.stdout.take().unwrap();let stderr=child.stderr.take().unwrap();let maximum=self.maximum_stdout;let mut out=ReaderGuard(tokio::spawn(output(stdout,maximum,false)));let mut err=ReaderGuard(tokio::spawn(output(stderr,4096,true)));
  let expiry=async{if let Some(d)=work.lifetime{tokio::time::sleep(d).await}else{std::future::pending().await}};
  tokio::pin!(expiry);
  let mut early=None;
  let status=tokio::select!{biased;_=cancel.cancelled()=>{let _=child.kill().await;return Ok(None);},_=&mut expiry=>{let _=child.kill().await;return Ok(None);},result=&mut out.0=>{early=Some(result.map_err(|e|Error::Worker(e.to_string()))??);tokio::select!{_=cancel.cancelled()=>{let _=child.kill().await;return Ok(None);},_=&mut expiry=>{let _=child.kill().await;return Ok(None);},s=child.wait()=>s?}},s=child.wait()=>s?};
  // The direct child may have left descendants holding pipe ends. Always tear
  // down its isolated process group before the bounded final drain.
  #[cfg(unix)]drop(group);
  let bytes=if let Some(bytes)=early{bytes}else{tokio::time::timeout(self.drain_timeout,&mut out.0).await.map_err(|_|Error::Worker("stdout drain deadline exceeded".into()))?.map_err(|e|Error::Worker(e.to_string()))??};
  let stderr=tokio::time::timeout(self.drain_timeout,&mut err.0).await.map_err(|_|Error::Worker("stderr drain deadline exceeded".into()))?.map_err(|e|Error::Worker(e.to_string()))??;
  if !status.success(){return Err(Error::Worker(format!("exit {status}: {}",String::from_utf8_lossy(&stderr))));}
  #[derive(serde::Deserialize)]#[serde(rename_all="camelCase")]struct Output {work_id:String,status:String,nonce:Option<u64>,range_start:u64,range_count:u64}
  let result:Output=serde_json::from_slice(&bytes).map_err(|_|Error::Worker(format!("invalid worker JSON: {}",String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(4096)..]))))?;
  if result.work_id!=work.work_id||result.range_start!=range.start_nonce||result.range_count!=range.count{return Err(Error::Worker("worker assignment identity/range mismatch".into()));}
  match result.status.as_str(){"found"=>{let nonce=result.nonce.ok_or_else(||Error::Worker("found without nonce".into()))?;if !range.contains(nonce){return Err(Error::Worker("nonce outside assignment".into()));}Ok(Some(WorkerResult{worker_id:self.id.clone(),work_id:result.work_id,nonce}))},"exhausted"=>{if result.nonce.is_some(){return Err(Error::Worker("exhausted with nonce".into()));}Ok(None)},_=>Err(Error::Worker("unknown worker status".into()))}
 }.boxed()
    }
}
