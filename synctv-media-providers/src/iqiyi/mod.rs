use crate::web_session::{ScopedWebSessionClient, SessionCookie};
use crate::ProviderClientError;

pub const IQIYI_SESSION_DOMAINS: &[&str] = &["iqiyi.com"];

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

    /// Fetch an iQiyi web resource using the authenticated server-side session.
    ///
    /// This primitive deliberately does not decrypt DRM media or synthesize
    /// playback licenses. Provider-specific playback code may only consume
    /// upstream resources that the authenticated account is legitimately
    /// authorized to request.
    pub async fn fetch_page(&self, url: &str) -> Result<String, ProviderClientError> {
        self.session.get_text(url).await
    }
}
