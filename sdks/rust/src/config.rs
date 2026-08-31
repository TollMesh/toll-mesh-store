/// Client configuration
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub timeout: Duration,
    pub verify_ssl: bool,
    pub scheme: String,
    pub api_key: Option<String>,
    pub max_retries: u32,
    pub retry_backoff: f64,
    pub pool_size: usize,
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = scheme.into();
        self
    }

    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8080,
            timeout: Duration::from_secs(5),
            verify_ssl: true,
            scheme: "http".to_string(),
            api_key: None,
            max_retries: 3,
            retry_backoff: 2.0,
            pool_size: 10,
        }
    }
}
