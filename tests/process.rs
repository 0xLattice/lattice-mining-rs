#![cfg(unix)]
use lattice_mining::*;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
fn work() -> Arc<Work> {
    Arc::new(Work {
        work_id: "fixed".into(),
        block_hex: "00".into(),
        prefix: vec![1],
        target: [255; 32],
        targets: vec![[255; 32]],
        lifetime: Some(Duration::from_millis(150)),
        stale_token: "stable".into(),
    })
}
async fn run(code: &str) -> Result<Option<WorkerResult>, Error> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker.py");
    std::fs::write(&path, format!("import sys,json,time,os\n{code}\n")).unwrap();
    let mut worker = ProcessWorker::new("p", "/usr/bin/python3".into());
    worker.arguments = vec![path.to_string_lossy().into_owned()];
    tokio::time::timeout(
        Duration::from_secs(2),
        worker.search(
            work(),
            NonceRange {
                start_nonce: u64::MAX,
                count: 2,
            },
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn validates_complete_assignment_and_wrapping_nonce() {
    let valid = r#"print(json.dumps({'workId':'fixed','status':'found','nonce':0,'rangeStart':18446744073709551615,'rangeCount':2}))"#;
    assert_eq!(run(valid).await.unwrap().unwrap().nonce, 0);
    for code in [
        valid.replace("'fixed'", "'other'"),
        valid.replace("'rangeCount':2", "'rangeCount':3"),
        valid.replace("'nonce':0", "'nonce':1"),
        valid.replace("'found'", "'invented'"),
        format!("{valid}\n{valid}"),
    ] {
        assert!(run(&code).await.is_err(), "{code}");
    }
}
#[tokio::test]
async fn stderr_flood_is_drained_and_tail_bounded() {
    let e = run("sys.stderr.write('x'*200000+'TAIL');sys.stderr.flush();sys.exit(7)")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("TAIL"));
    assert!(e.len() < 4300);
}
#[tokio::test]
async fn stdout_flood_and_deadline_fail_bounded() {
    assert!(
        run("sys.stdout.write('x'*200000);sys.stdout.flush();time.sleep(10)")
            .await
            .is_err()
    );
    assert!(
        run("sys.stdout.close();os.close(1);time.sleep(10)")
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn cancellation_kills_descendant_group() {
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("pid");
    let script = dir.path().join("fork.py");
    std::fs::write(&script,format!("import os,time\np=os.fork()\nif p==0:\n open({:?},'w').write(str(os.getpid()))\n time.sleep(30)\nelse: time.sleep(30)\n",pidfile.to_str().unwrap())).unwrap();
    let mut worker = ProcessWorker::new("p", "/usr/bin/python3".into());
    worker.arguments = vec![script.to_str().unwrap().into()];
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let mut w = (*work()).clone();
    w.lifetime = None;
    let task = tokio::spawn(async move {
        worker
            .search(
                Arc::new(w),
                NonceRange {
                    start_nonce: 0,
                    count: 1,
                },
                token,
            )
            .await
    });
    for _ in 0..100 {
        if pidfile.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(pidfile.exists());
    cancel.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none()
    );
    let pid: i32 = std::fs::read_to_string(pidfile).unwrap().parse().unwrap();
    for _ in 0..100 {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&output.stdout);
        if state.trim().is_empty() || state.trim().starts_with('Z') {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("descendant survived cancellation");
}
