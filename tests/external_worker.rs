use lattice_mining::*;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[tokio::test]
#[ignore = "requires LATTICE_TEST_WORKER pointing to the separately built worker"]
async fn actual_external_cpu_and_automatic_backend_wrapping_contract() {
    let path = std::env::var_os("LATTICE_TEST_WORKER").expect("set LATTICE_TEST_WORKER");
    for backend in ["cpu", "auto"] {
        let mut worker = ProcessWorker::new("external", path.clone().into());
        worker.arguments = vec!["--backend".into(), backend.into()];
        let work = Arc::new(Work {
            work_id: "offline-integration".into(),
            block_hex: "00".into(),
            prefix: b"offline fixture only".to_vec(),
            target: [255; 32],
            targets: vec![[255; 32]],
            lifetime: Some(Duration::from_secs(10)),
            stale_token: "fixed".into(),
        });
        let range = NonceRange {
            start_nonce: u64::MAX,
            count: 2,
        };
        let result = worker
            .search(work.clone(), range, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert!(range.contains(result.nonce));
        assert_eq!(result.work_id, work.work_id);
        assert!(lattice_pow::hash(&work.prefix, result.nonce) <= work.target);
        let mut impossible = (*work).clone();
        impossible.target = [0; 32];
        assert!(
            worker
                .search(Arc::new(impossible), range, CancellationToken::new())
                .await
                .unwrap()
                .is_none()
        );
    }
}
