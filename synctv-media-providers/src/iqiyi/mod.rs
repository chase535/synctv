use crate::web_session::{ScopedWebSessionClient, SessionCookie, WebPagePlaybackDiscovery};
use crate::ProviderClientError;

pub const IQIYI_SESSION_DOMAINS: &[&str] = &["iqiyi.com", "qiyi.com"];

#[derive(Clone)]
pub struct IqiyiClient {
    session: ScopedWebSessionClient,
}

impl IqiyiClient {
    pub fn new(
        client: reqwest::Client,
        cookies: Vec<SessionCookie>,
    ) -> Result<Self, ProviderClientError> {
        Ok(Self {
            session: ScopedWebSessionClient::new(client, IQIYI_SESSION_DOMAINS, cookies)?,
        })
    }

    #[must_use]
    pub fn cookies(&self) -> &[SessionCookie] {
        self.session.cookies()
    }

    pub fn validate_url(&self, url: &str) -> Result<url::Url, ProviderClientError> {
        self.session.validate_url(url)
    }

    /// Fetch an iQiyi web resource using the authenticated server-side session.
    ///
    /// This primitive deliberately does not decrypt DRM media or synthesize
    /// playback licenses. Provider-specific playback code may only consume
    /// upstream resources that the authenticated account is legitimately
    /// authorized to request.
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
