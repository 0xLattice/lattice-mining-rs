use lattice_pow::Target;
use lattice_primitives::Block;
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Configuration(String),
    #[error("node authorization failed: HTTP {0}")]
    Unauthorized(u16),
    #[error("invalid node response: HTTP {0}")]
    Response(u16),
    #[error("HTTP request: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("content: {0}")]
    Content(#[from] lattice_cashew::Error),
    #[error("worker: {0}")]
    Worker(String),
    #[error("cancelled")]
    Cancelled,
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonceRange {
    pub start_nonce: u64,
    pub count: u64,
}
impl NonceRange {
    pub fn contains(&self, nonce: u64) -> bool {
        nonce.wrapping_sub(self.start_nonce) < self.count
    }
}
pub fn allocate_ranges(total: u64, workers: usize, offset: u64) -> Vec<NonceRange> {
    let workers = workers.max(1);
    let total = total.max(1);
    let base = total / (workers as u64);
    let rem = total % (workers as u64);
    let mut at = offset;
    let mut ranges = vec![];
    for i in 0..workers {
        let count = base + u64::from((i as u64) < rem);
        if count > 0 {
            ranges.push(NonceRange {
                start_nonce: at,
                count,
            });
            at = at.wrapping_add(count);
        }
    }
    ranges
}
pub fn parse_target(s: &str) -> Option<Target> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() || s.len() > 64 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    hex::decode(format!("{s:0>64}")).ok()?.try_into().ok()
}
pub fn parse_minimum_work(s: &str) -> Option<Target> {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    let n = if let Some(exp) = s.strip_prefix("2^") {
        if !digits(exp) {
            return None;
        }
        let shift = exp.parse::<usize>().ok()?;
        if shift >= 256 {
            return None;
        }
        BigUint::from(1u8) << shift
    } else {
        if !digits(s) {
            return None;
        }
        BigUint::parse_bytes(s.as_bytes(), 10)?
    };
    if n == BigUint::from(0u8) || n > (BigUint::from(1u8) << 255usize) {
        return None;
    }
    let bytes = n.to_bytes_be();
    let mut out = [0; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Some(out)
}
fn fields(entries: &[String], work: bool) -> Result<Vec<serde_json::Value>, Error> {
    let mut seen = BTreeSet::new();
    entries.iter().map(|e|{let(path,value)=e.split_once('=').ok_or_else(||Error::Configuration("expected chain=value".into()))?;let chain:Vec<_>=path.split('/').collect();if chain.first()!=Some(&"Nexus")||chain.iter().any(|x|x.is_empty())||value.is_empty()||!seen.insert(path){return Err(Error::Configuration("invalid or duplicate chain assignment".into()));}
if work{let n=parse_minimum_work(value).ok_or_else(||Error::Configuration("invalid minimum work".into()))?;Ok(serde_json::json!({"chainPath":chain,"work":format!("0x{}",hex::encode(n).trim_start_matches('0'))}))}else{Ok(serde_json::json!({"chainPath":chain,"address":value}))}}).collect()
}
pub fn template_request(
    recipients: &[String],
    minimum_work: &[String],
) -> Result<serde_json::Value, Error> {
    let mut v = serde_json::json!({"recipients":fields(recipients,false)?});
    if !minimum_work.is_empty() {
        v["minimumWork"] = serde_json::json!(fields(minimum_work, true)?);
    }
    Ok(v)
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateResponse {
    #[serde(rename = "workID")]
    pub work_id: String,
    pub block: Block,
    pub search_target: lattice_primitives::UInt256,
    pub targets: Vec<lattice_primitives::UInt256>,
    pub chain_path: Vec<String>,
    pub expires_in_milliseconds: u64,
    pub template_digest: String,
}
/// Immutable assignment data, constructed only after canonical block encoding.
#[derive(Debug, Clone)]
pub struct Work {
    pub work_id: String,
    pub block_hex: String,
    pub prefix: Vec<u8>,
    pub target: Target,
    pub targets: Vec<Target>,
    pub lifetime: Option<std::time::Duration>,
    pub stale_token: String,
}
impl TryFrom<TemplateResponse> for Work {
    type Error = Error;
    fn try_from(t: TemplateResponse) -> Result<Self, Error> {
        if t.work_id.is_empty() || t.template_digest.is_empty() {
            return Err(Error::Configuration("empty work identity/digest".into()));
        }
        let target = t.search_target.0;
        let mut targets: BTreeSet<_> = t
            .targets
            .into_iter()
            .map(|x| x.0)
            .filter(|x| *x <= target)
            .collect();
        targets.insert(target);
        Ok(Self {
            work_id: t.work_id,
            block_hex: hex::encode(lattice_cashew::cbor::encode(&t.block)?),
            prefix: t.block.pow_prefix(),
            target,
            targets: targets.into_iter().rev().collect(),
            lifetime: Some(std::time::Duration::from_millis(t.expires_in_milliseconds)),
            stale_token: t.template_digest,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Submission {
    pub accepted: bool,
    pub disposition: String,
    #[serde(rename = "tipCID", skip_serializing_if = "Option::is_none")]
    pub tip_cid: Option<String>,
}
#[derive(Debug, Clone)]
pub struct WorkerResult {
    pub worker_id: String,
    pub work_id: String,
    pub nonce: u64,
}
