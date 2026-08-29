use crate::web_session::{ScopedWebSessionClient, SessionCookie, WebPagePlaybackDiscovery};
use crate::ProviderClientError;

pub const TENCENT_VIDEO_SESSION_DOMAINS: &[&str] = &["qq.com"];

#[derive(Clone)]
pub struct TencentVideoClient {
    session: ScopedWebSessionClient,
}

impl TencentVideoClient {
    pub fn new(
        client: reqwest::Client,
        cookies: Vec<SessionCookie>,
    ) -> Result<Self, ProviderClientError> {
        Ok(Self {
            session: ScopedWebSessionClient::new(client, TENCENT_VIDEO_SESSION_DOMAINS, cookies)?,
        })
    }

    #[must_use]
    pub fn cookies(&self) -> &[SessionCookie] {
        self.session.cookies()
    }

    pub fn validate_url(&self, url: &str) -> Result<url::Url, ProviderClientError> {
        self.session.validate_url(url)
    }

    /// Fetch a Tencent Video web resource using the authenticated server-side
    /// session without exporting that session to room members.
    ///
    /// This primitive does not bypass DRM, device authorization, or concurrency
    /// enforcement and does not manufacture playback licenses.
    pub async fn fetch_page(&self, url: &str) -> Result<String, ProviderClientError> {
        self.session.get_text(url).await
    }

    /// Discover direct HTTP(S) media explicitly exposed by the authenticated page.
    ///
    /// This does not call private signing endpoints, derive device credentials,
    /// decrypt DRM, or construct license requests.
    pub async fn discover_playback(
        &self,
        url: &str,
    ) -> Result<WebPagePlaybackDiscovery, ProviderClientError> {
        self.session.discover_page_playback(url).await
    }
}
