use lattice_mining::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
async fn server(status: u16, body: String) -> (String, tokio::task::JoinHandle<String>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut bytes = vec![];
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
        let head = String::from_utf8(bytes.clone()).unwrap();
        let len = head
            .lines()
            .find_map(|l| {
                l.to_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|n| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut bodyin = vec![0; len];
        s.read_exact(&mut bodyin).await.unwrap();
        bytes.extend(bodyin);
        let head = format!(
            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(body.as_bytes()).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (url, task)
}
fn client(url: &str) -> HttpNodeClient {
    HttpNodeClient::new(
        url,
        serde_json::json!({"recipients":[]}),
        None,
        Duration::from_secs(1),
        1024,
    )
    .unwrap()
}
#[tokio::test]
async fn status_digest_fallback_and_template_refusal() {
    for (body, expected) in [
        (r#"{"templateDigest":"new","tipCID":"old"}"#, Some("new")),
        (r#"{"templateDigest":"","tipCID":"old"}"#, Some("old")),
        ("{}", None),
    ] {
        let (url, t) = server(200, body.into()).await;
        assert_eq!(
            client(&url).stale_token().await.unwrap().as_deref(),
            expected
        );
        assert!(t.await.unwrap().starts_with("GET /status "));
    }
    for status in [409, 503] {
        let (url, t) = server(status, "{}".into()).await;
        assert!(client(&url).fetch_work().await.unwrap().is_none());
        let request = t.await.unwrap();
        assert!(request.starts_with("POST /mining/templates "));
        assert!(request.ends_with(r#"{"recipients":[]}"#));
    }
}
#[tokio::test]
async fn submit_contract_cookie_and_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join("cookie");
    std::fs::write(&cookie, "__cookie__:new\n").unwrap();
    let (url, t) = server(409, r#"{"error":{"message":"expired"}}"#.into()).await;
    let c = HttpNodeClient::new(
        &url,
        serde_json::json!({}),
        Some(cookie),
        Duration::from_secs(1),
        1024,
    )
    .unwrap();
    let result = c.submit("work".into(), u64::MAX).await.unwrap();
    assert!(!result.accepted);
    assert_eq!(result.disposition, "expired");
    let request = t.await.unwrap();
    assert!(request.starts_with("POST /mining/work "));
    assert!(request.contains("authorization: Basic X19jb29raWVfXzpuZXc="));
    let body: serde_json::Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["nonce"], u64::MAX);
    assert_eq!(body["workID"], "work");
}
#[test]
fn decode_http_and_url_policy() {
    for status in [401, 403] {
        assert!(matches!(
            decode_submission(status, b"{}"),
            Err(Error::Unauthorized(_))
        ));
    }
    for status in [500, 503] {
        assert!(
            decode_submission(status, br#"{"accepted":true,"disposition":"accepted"}"#).is_err()
        );
    }
    assert!(decode_submission(409, br#"{"accepted":false,"disposition":"stale"}"#).is_ok());
    assert!(decode_submission(200, b"not json").is_err());
    for url in [
        "http://public.example",
        "https://user:pass@example.com",
        "https://a.example/?token=x",
    ] {
        assert!(
            HttpNodeClient::new(
                url,
                serde_json::json!({}),
                None,
                Duration::from_secs(1),
                1024
            )
            .is_err()
        );
    }
}
#[tokio::test]
async fn bounded_reply_and_no_redirect() {
    let (url, t) = server(200, "x".repeat(2000)).await;
    assert!(client(&url).stale_token().await.is_err());
    t.await.unwrap();
    let (url, t) = server(302, "{}".into()).await;
    assert!(client(&url).stale_token().await.unwrap().is_none());
    t.await.unwrap();
}
