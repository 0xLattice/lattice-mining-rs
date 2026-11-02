use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_primitives::*;
use std::collections::BTreeMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
fn block() -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: None,
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume::new(ChainSpec {
            max_number_of_transactions_per_block: 100,
            max_state_growth: 10000,
            max_block_size: 1000000,
            premine: 0,
            target_block_time: 1000,
            initial_reward: 1,
            halving_interval: 100,
            half_life: 120,
            wasm_policies: vec![],
        })
        .unwrap(),
        parent_state: empty.clone(),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: 0,
        timestamp: 0,
        reward_recipient: None,
        nonce: 0,
    }
}
#[tokio::test]
async fn once_cli_real_cpu_worker_local_template_and_submit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let original = block();
    let prefix = original.pow_prefix();
    let server = tokio::spawn(async move {
        for step in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let mut b = [0];
            while !bytes.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut b).await.unwrap();
                bytes.push(b[0]);
            }
            let headers = String::from_utf8(bytes).unwrap();
            let len = headers
                .lines()
                .find_map(|l| {
                    l.to_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|n| n.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            let mut body = vec![0; len];
            socket.read_exact(&mut body).await.unwrap();
            let input: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let response = if step == 0 {
                assert!(headers.starts_with("POST /mining/templates "));
                assert_eq!(input, serde_json::json!({"recipients":[]}));
                serde_json::json!({"workID":"local-fixture","block":original,"searchTarget":"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","targets":["0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],"chainPath":["Nexus"],"expiresInMilliseconds":5000,"templateDigest":"stable"})
            } else {
                assert!(headers.starts_with("POST /mining/work "));
                assert_eq!(input["workID"], "local-fixture");
                let nonce = input["nonce"].as_u64().unwrap();
                assert!(nonce < 2);
                assert!(lattice_pow::hash(&prefix, nonce) <= [255; 32]);
                serde_json::json!({"accepted":true,"disposition":"accepted","tipCID":"fixture-tip"})
            };
            let body = serde_json::to_vec(&response).unwrap();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_lattice-mining-coordinator"))
            .args([
                "--node",
                &url,
                "--once",
                "--no-stale-probe",
                "--workers",
                "2",
                "--batch-size",
                "2",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["accepted"], true);
    assert_eq!(result["result"], "submitted");
    server.await.unwrap();
}
