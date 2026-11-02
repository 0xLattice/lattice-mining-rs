use crate::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{FutureExt, future::BoxFuture};
use std::{path::PathBuf, time::Duration};
#[derive(Clone)]
pub struct HttpNodeClient {
    base: url::Url,
    request: serde_json::Value,
    cookie: Option<PathBuf>,
    client: reqwest::Client,
    maximum_bytes: usize,
}
impl HttpNodeClient {
    pub fn new(
        base: &str,
        request: serde_json::Value,
        cookie: Option<PathBuf>,
        timeout: Duration,
        maximum_bytes: usize,
    ) -> Result<Self, Error> {
        let mut base = url::Url::parse(base).map_err(|e| Error::Configuration(e.to_string()))?;
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::Configuration(
                "node URL credentials/query/fragment forbidden".into(),
            ));
        }
        let loopback = match base.host() {
            Some(url::Host::Domain(h)) => h.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if base.scheme() != "https" && !(base.scheme() == "http" && loopback) {
            return Err(Error::Configuration(
                "HTTPS required except loopback HTTP".into(),
            ));
        }
        if timeout.is_zero() || maximum_bytes == 0 {
            return Err(Error::Configuration(
                "positive deadline/body bound required".into(),
            ));
        }
        let path = format!("{}/", base.path().trim_end_matches('/'));
        base.set_path(&path);
        Ok(Self {
            base,
            request,
            cookie,
            client: reqwest::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            maximum_bytes,
        })
    }
    async fn request(
        &self,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<(u16, Vec<u8>), Error> {
        let url = self
            .base
            .join(path)
            .map_err(|e| Error::Configuration(e.to_string()))?;
        let mut request = if let Some(body) = body {
            self.client.post(url).json(&body)
        } else {
            self.client.get(url)
        };
        if let Some(file) = &self.cookie
            && let Ok(content) = tokio::fs::read_to_string(file).await
        {
            let cookie = content.trim();
            if !cookie.is_empty() {
                request = request.header(
                    reqwest::header::AUTHORIZATION,
                    format!("Basic {}", STANDARD.encode(cookie)),
                );
            }
        }
        let mut response = request.send().await?;
        let status = response.status().as_u16();
        if status == 401 || status == 403 {
            return Err(Error::Unauthorized(status));
        }
        if response
            .content_length()
            .is_some_and(|n| n > self.maximum_bytes as u64)
        {
            return Err(Error::Response(status));
        }
        let mut bytes = vec![];
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > self.maximum_bytes - bytes.len() {
                return Err(Error::Response(status));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((status, bytes))
    }
}
pub fn decode_submission(status: u16, bytes: &[u8]) -> Result<Submission, Error> {
    if status == 401 || status == 403 {
        return Err(Error::Unauthorized(status));
    }
    if status >= 500 {
        return Err(Error::Response(status));
    }
    if let Ok(result) = serde_json::from_slice::<Submission>(bytes) {
        return Ok(result);
    }
    if status >= 400 {
        #[derive(serde::Deserialize)]
        struct Refusal {
            error: Reason,
        }
        #[derive(serde::Deserialize)]
        struct Reason {
            message: String,
        }
        if let Ok(r) = serde_json::from_slice::<Refusal>(bytes) {
            return Ok(Submission {
                accepted: false,
                disposition: r.error.message,
                tip_cid: None,
            });
        }
    }
    Err(Error::Response(status))
}
impl NodeClient for HttpNodeClient {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async move {
            let (status, bytes) = self
                .request("mining/templates", Some(self.request.clone()))
                .await?;
            match status {
                409 | 503 => Ok(None),
                200 => Ok(Some(
                    serde_json::from_slice::<TemplateResponse>(&bytes)?.try_into()?,
                )),
                _ => Err(Error::Response(status)),
            }
        }
        .boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        async move {
            let (status, bytes) = self.request("status", None).await?;
            if status != 200 {
                return Ok(None);
            }
            #[derive(serde::Deserialize)]
            struct Status {
                #[serde(rename = "tipCID")]
                tip: Option<String>,
                #[serde(rename = "templateDigest")]
                digest: Option<String>,
            }
            let s: Status = serde_json::from_slice(&bytes).map_err(|_| Error::Response(status))?;
            Ok(s.digest
                .filter(|s| !s.is_empty())
                .or_else(|| s.tip.filter(|s| !s.is_empty())))
        }
        .boxed()
    }
    fn submit(&self, work_id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        async move {
            let (status, bytes) = self
                .request(
                    "mining/work",
                    Some(serde_json::json!({"workID":work_id,"nonce":nonce})),
                )
                .await?;
            decode_submission(status, &bytes)
        }
        .boxed()
    }
}
