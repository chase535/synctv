use synctv_proto::providers::common::{BindWebSessionRequest, WebSessionCookie};
use synctv_proto::source_config::SourceProvider;

#[test]
fn web_session_provider_codes_are_stable() {
    assert_eq!(SourceProvider::Iqiyi as i32, 22);
    assert_eq!(SourceProvider::TencentVideo as i32, 23);
}

#[test]
fn bind_web_session_request_carries_cookie_material_only_at_bind_boundary() {
    let request = BindWebSessionRequest {
        provider: SourceProvider::Iqiyi as i32,
        label: "official session".to_string(),
        cookies: vec![WebSessionCookie {
            name: "session".to_string(),
            value: "secret".to_string(),
            domain: ".iqiyi.com".to_string(),
            path: "/".to_string(),
            secure: true,
            http_only: true,
            session_only: true,
            expires_at: None,
        }],
    };

    assert_eq!(request.provider, SourceProvider::Iqiyi as i32);
    assert_eq!(request.cookies.len(), 1);
    assert_eq!(request.cookies[0].domain, ".iqiyi.com");
    assert!(request.cookies[0].secure);
    assert!(request.cookies[0].http_only);
}
