use futures_util::{FutureExt, future::BoxFuture};
use lattice_mining::*;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn work() -> Work {
    Work {
        work_id: "one".into(),
        block_hex: "00".into(),
        prefix: b"fixture".to_vec(),
        target: [255; 32],
        targets: vec![[255; 32]],
        lifetime: None,
        stale_token: "stable".into(),
    }
}
struct Node {
    works: Mutex<VecDeque<Option<Work>>>,
    tokens: Mutex<VecDeque<i32>>,
    answers: Mutex<VecDeque<i32>>,
    submits: Mutex<Vec<(String, u64)>>,
    fetches: AtomicUsize,
}
impl Node {
    fn new(w: Work) -> Self {
        Self {
            works: Mutex::new(VecDeque::from([Some(w)])),
            tokens: Mutex::new(VecDeque::new()),
            answers: Mutex::new(VecDeque::new()),
            submits: Mutex::new(vec![]),
            fetches: AtomicUsize::new(0),
        }
    }
}
impl NodeClient for Node {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(self.works.lock().unwrap().pop_front().flatten())
        }
        .boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        async {
            match self.tokens.lock().unwrap().pop_front().unwrap_or(0) {
                1 => Ok(Some("changed".into())),
                2 => Err(Error::Response(503)),
                _ => Ok(Some("stable".into())),
            }
        }
        .boxed()
    }
    fn submit(&self, id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        async move {
            self.submits.lock().unwrap().push((id, nonce));
            match self.answers.lock().unwrap().pop_front().unwrap_or(0) {
                1 => Err(Error::Response(503)),
                2 => Err(Error::Unauthorized(401)),
                3 => Ok(Submission {
                    accepted: false,
                    disposition: "childOnly".into(),
                    tip_cid: None,
                }),
                4 => Ok(Submission {
                    accepted: false,
                    disposition: "refused".into(),
                    tip_cid: None,
                }),
                _ => Ok(Submission {
                    accepted: true,
                    disposition: "accepted".into(),
                    tip_cid: Some("tip".into()),
                }),
            }
        }
        .boxed()
    }
}
#[derive(Default)]
struct Searcher {
    assignments: Mutex<Vec<(String, [u8; 32], NonceRange)>>,
    delay: Duration,
    fail: bool,
    none: bool,
    wrong: bool,
}
impl Worker for Searcher {
    fn id(&self) -> &str {
        "test"
    }
    fn search(
        &self,
        w: Arc<Work>,
        r: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move{self.assignments.lock().unwrap().push((w.work_id.clone(),w.target,r));tokio::select!{_=cancel.cancelled()=>return Ok(None),_=tokio::time::sleep(self.delay)=>{}}if self.fail{return Err(Error::Worker("fixture failure".into()));}
if self.none{return Ok(None);}for i in 0..r.count {let nonce=r.start_nonce.wrapping_add(i);if lattice_pow::hash(&w.prefix,nonce)<=w.target{return Ok(Some(WorkerResult{worker_id:"test".into(),work_id:if self.wrong{"wrong".into()}else{w.work_id.clone()},nonce}));}}Ok(None)}.boxed()
    }
}
fn opts() -> CoordinatorOptions {
    CoordinatorOptions {
        total_batch_size: 20,
        stale_probe: false,
        retry_delay: Duration::ZERO,
        ..Default::default()
    }
}
#[test]
fn partition_wrap_and_configuration() {
    assert_eq!(
        allocate_ranges(5, 3, u64::MAX - 1),
        vec![
            NonceRange {
                start_nonce: u64::MAX - 1,
                count: 2
            },
            NonceRange {
                start_nonce: 0,
                count: 2
            },
            NonceRange {
                start_nonce: 2,
                count: 1
            }
        ]
    );
    assert_eq!(
        allocate_ranges(0, 0, 7),
        vec![NonceRange {
            start_nonce: 7,
            count: 1
        }]
    );
    assert_eq!(allocate_ranges(2, 9, 0).len(), 2);
    assert!(parse_minimum_work("2^255").is_some());
    assert!(parse_minimum_work("2^256").is_none());
    assert!(parse_minimum_work("0").is_none());
    assert_eq!(parse_target("0xF"), parse_target("f"));
    assert!(template_request(&["Nexus=a".into(), "Nexus=b".into()], &[]).is_err());
}
#[tokio::test]
async fn retry_same_nonce_and_clean_refusal() {
    for (answer, tries, accepted) in [(1, 2, true), (2, 1, false), (4, 1, false)] {
        let n = Arc::new(Node::new(work()));
        n.answers.lock().unwrap().push_back(answer);
        let mut c = Coordinator::new(n.clone(), vec![Arc::new(Searcher::default())], opts());
        let r = c.run_batch(&CancellationToken::new()).await;
        assert_eq!(n.submits.lock().unwrap().len(), tries);
        if answer == 2 {
            assert!(matches!(r, Cycle::NodeFailed { .. }));
        } else {
            assert!(matches!(r,Cycle::Submitted{submission,..}if submission.accepted==accepted));
        }
        assert!(
            n.submits
                .lock()
                .unwrap()
                .iter()
                .all(|(_, nonce)| *nonce == 0)
        );
    }
}
#[tokio::test]
async fn retries_bounded_and_stale_between_attempts() {
    let n = Arc::new(Node::new(work()));
    *n.answers.lock().unwrap() = VecDeque::from([1, 1, 1, 1, 1]);
    let mut c = Coordinator::new(n.clone(), vec![Arc::new(Searcher::default())], opts());
    assert!(
        matches!(c.run_batch(&CancellationToken::new()).await,Cycle::Submitted{submission,..}if submission.disposition=="submitFailed")
    );
    assert_eq!(n.submits.lock().unwrap().len(), 4);
    assert_eq!(c.metrics().retry_backoff_count, 3);
    let n = Arc::new(Node::new(work()));
    n.answers.lock().unwrap().push_back(1);
    n.tokens.lock().unwrap().push_back(1);
    let mut c = Coordinator::new(n.clone(), vec![Arc::new(Searcher::default())], opts());
    assert!(matches!(
        c.run_batch(&CancellationToken::new()).await,
        Cycle::Stale { .. }
    ));
    assert_eq!(n.submits.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn child_only_resumes_same_range_harder() {
    let mut w = work();
    let first = lattice_pow::hash(&w.prefix, 0);
    let (last, hard) = (1..1000)
        .map(|n| (n, lattice_pow::hash(&w.prefix, n)))
        .find(|(_, h)| *h < first)
        .unwrap();
    w.targets.push(hard);
    let n = Arc::new(Node::new(w));
    n.answers.lock().unwrap().push_back(3);
    let worker = Arc::new(Searcher::default());
    let mut c = Coordinator::new(
        n.clone(),
        vec![worker.clone()],
        CoordinatorOptions {
            total_batch_size: last + 1,
            ..opts()
        },
    );
    assert!(
        matches!(c.run_batch(&CancellationToken::new()).await,Cycle::Submitted{nonce,submission,..}if nonce==last&&submission.accepted)
    );
    let a = worker.assignments.lock().unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(a[1].1, hard);
    assert_eq!(
        a[1].2,
        NonceRange {
            start_nonce: 1,
            count: last
        }
    );
    assert_eq!(n.submits.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn offset_tracks_same_work_and_resets_changed_work() {
    let n = Arc::new(Node::new(work()));
    let mut second = work();
    second.work_id = "two".into();
    n.works.lock().unwrap().extend([Some(work()), Some(second)]);
    let worker = Arc::new(Searcher {
        none: true,
        ..Default::default()
    });
    let mut c = Coordinator::new(
        n,
        vec![worker.clone()],
        CoordinatorOptions {
            nonce_offset: u64::MAX - 5,
            total_batch_size: 8,
            ..opts()
        },
    );
    for _ in 0..3 {
        assert!(matches!(
            c.run_batch(&CancellationToken::new()).await,
            Cycle::NoSolution { .. }
        ));
    }
    let a = worker.assignments.lock().unwrap();
    assert_eq!(
        a.iter().map(|x| x.2.start_nonce).collect::<Vec<_>>(),
        vec![u64::MAX - 5, 2, 0]
    );
}
#[tokio::test]
async fn stale_expiry_failures_and_wrong_identity() {
    for mode in 0..5 {
        let mut w = work();
        if mode == 1 {
            w.lifetime = Some(Duration::from_millis(2));
        }
        let n = Arc::new(Node::new(w));
        if mode == 0 {
            n.tokens.lock().unwrap().push_back(1);
        }
        let worker = Arc::new(Searcher {
            delay: Duration::from_millis(if mode <= 1 { 50 } else { 0 }),
            fail: mode == 2,
            wrong: mode == 3,
            none: mode == 4,
            ..Default::default()
        });
        let mut c = Coordinator::new(
            n.clone(),
            vec![worker],
            CoordinatorOptions {
                stale_probe: mode == 0,
                ..opts()
            },
        );
        let r = tokio::time::timeout(
            Duration::from_secs(1),
            c.run_batch(&CancellationToken::new()),
        )
        .await
        .unwrap();
        match mode {
            0 | 1 => assert!(matches!(r, Cycle::Stale { .. })),
            2 => assert!(matches!(r, Cycle::WorkerFailed { .. })),
            _ => assert!(matches!(r, Cycle::NoSolution { .. })),
        }
        assert!(n.submits.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn repeat_probe_and_pre_submit_probe() {
    let n = Arc::new(Node::new(work()));
    *n.tokens.lock().unwrap() = VecDeque::from([0, 1]);
    let mut c = Coordinator::new(
        n.clone(),
        vec![Arc::new(Searcher {
            delay: Duration::from_millis(50),
            ..Default::default()
        })],
        CoordinatorOptions {
            stale_probe: true,
            probe_interval: Duration::from_millis(2),
            ..opts()
        },
    );
    assert!(matches!(
        c.run_batch(&CancellationToken::new()).await,
        Cycle::Stale { .. }
    ));
    assert!(n.submits.lock().unwrap().is_empty());
}
#[tokio::test]
async fn cancellation_ends_batch_and_owned_loop() {
    let n = Arc::new(Node::new(work()));
    let c = Coordinator::new(
        n.clone(),
        vec![Arc::new(Searcher {
            delay: Duration::from_secs(10),
            ..Default::default()
        })],
        opts(),
    );
    let cancel = CancellationToken::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let token = cancel.clone();
    let handle = tokio::spawn(c.run(token, tx));
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(n.submits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn actual_swift_core_and_cycle_differential() {
    let v: serde_json::Value = serde_json::from_str(include_str!("swift-vectors.json")).unwrap();
    assert_eq!(v["ranges"].as_array().unwrap().len(), 72);
    for row in v["ranges"].as_array().unwrap() {
        let n = |key: &str| row[key].as_str().unwrap().parse::<u64>().unwrap();
        let actual: Vec<_> = allocate_ranges(
            n("total"),
            row["workers"].as_u64().unwrap() as usize,
            n("offset"),
        )
        .into_iter()
        .map(|r| serde_json::json!({"start":r.start_nonce.to_string(),"count":r.count.to_string()}))
        .collect();
        assert_eq!(serde_json::json!(actual), row["ranges"]);
    }
    for row in v["parses"].as_array().unwrap() {
        let s = row["input"].as_str().unwrap();
        assert_eq!(
            serde_json::json!(parse_target(s).map(hex::encode)),
            row["target"],
            "target {s}"
        );
        assert_eq!(
            serde_json::json!(parse_minimum_work(s).map(hex::encode)),
            row["work"],
            "work {s}"
        );
    }
    assert_eq!(v["cycles"].as_array().unwrap().len(), 13);
    for row in v["cycles"].as_array().unwrap() {
        let mode = row["mode"].as_str().unwrap();
        let mut w = work();
        w.prefix.clear();
        let mut total = 8;
        if mode == "merged" {
            w.prefix = b"fixture".to_vec();
            let first = lattice_pow::hash(&w.prefix, 0);
            let (last, hard) = (1..1000)
                .map(|n| (n, lattice_pow::hash(&w.prefix, n)))
                .find(|(_, h)| *h < first)
                .unwrap();
            w.targets.push(hard);
            total = last + 1;
        }
        if mode == "expire" {
            w.lifetime = Some(Duration::from_millis(1));
        }
        let n = Arc::new(Node::new(w.clone()));
        match mode {
            "no_work" => *n.works.lock().unwrap() = VecDeque::from([None]),
            "rejected" => n.answers.lock().unwrap().push_back(4),
            "transient" => n.answers.lock().unwrap().push_back(1),
            "exhaust_retries" => *n.answers.lock().unwrap() = VecDeque::from([1, 1, 1, 1]),
            "unauthorized" => n.answers.lock().unwrap().push_back(2),
            "stale_retry" => {
                n.answers.lock().unwrap().push_back(1);
                n.tokens.lock().unwrap().push_back(1);
            }
            "merged" => n.answers.lock().unwrap().push_back(3),
            "offsets" => {
                let mut other = w.clone();
                other.work_id = "two".into();
                n.works.lock().unwrap().extend([Some(w), Some(other)]);
            }
            _ => {}
        }
        let worker = Arc::new(Searcher {
            delay: if mode == "expire" {
                Duration::from_millis(50)
            } else {
                Duration::ZERO
            },
            none: matches!(mode, "no_solution" | "offsets"),
            fail: mode == "worker_failure",
            wrong: mode == "wrong_id",
            ..Default::default()
        });
        let mut c = Coordinator::new(
            n.clone(),
            vec![worker.clone()],
            CoordinatorOptions {
                total_batch_size: total,
                nonce_offset: if mode == "offsets" { u64::MAX - 5 } else { 0 },
                ..opts()
            },
        );
        let mut results = vec![];
        for _ in 0..if mode == "offsets" { 3 } else { 1 } {
            let result = c.run_batch(&CancellationToken::new()).await;
            let mut r = serde_json::to_value(result).unwrap();
            let object = r.as_object_mut().unwrap();
            object.remove("error");
            object.remove("tipCID");
            if let Some(value) = object.get_mut("nonce") {
                *value = serde_json::json!(value.as_u64().unwrap().to_string());
            }
            if let Some(value) = object.get_mut("accepted") {
                *value = serde_json::json!(value.as_bool().unwrap().to_string());
            }
            results.push(r);
        }
        let assignments:Vec<_>=worker.assignments.lock().unwrap().iter().map(|(id,t,r)|serde_json::json!({"workId":id,"target":hex::encode(t),"start":r.start_nonce.to_string(),"count":r.count.to_string()})).collect();
        let m = c.metrics();
        let actual = serde_json::json!({"mode":mode,"results":results,"assignments":assignments,"submissions":n.submits.lock().unwrap().iter().map(|(_,n)|n.to_string()).collect::<Vec<_>>(),"metrics":{"assigned":m.assigned_range_count,"stale":m.stale_abort_count,"failure":m.worker_failure_count,"accepted":m.accepted_solution_count,"rejected":m.rejected_solution_count,"backoff":m.retry_backoff_count}});
        assert_eq!(&actual, row, "Swift cycle {mode}");
        if mode == "accepted" || mode == "merged" {
            assert_eq!(actual["results"][0]["accepted"], "true");
        }
    }
}
