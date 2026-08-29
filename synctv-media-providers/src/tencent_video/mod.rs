use crate::web_session::{ScopedWebSessionClient, SessionCookie};
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
            session: ScopedWebSessionClient::new(
                client,
                TENCENT_VIDEO_SESSION_DOMAINS,
                cookies,
            )?,
        })
    }

    #[must_use]
    pub fn cookies(&self) -> &[SessionCookie] {
        self.session.cookies()
    }

    /// Fetch a Tencent Video web resource using the authenticated server-side
    /// session without exporting that session to room members.
    ///
    /// This primitive does not bypass DRM, device authorization, or concurrency
    /// enforcement and does not manufacture playback licenses.
    pub async fn fetch_page(&self, url: &str) -> Result<String, ProviderClientError> {
        self.session.get_text(url).await
    }
}
