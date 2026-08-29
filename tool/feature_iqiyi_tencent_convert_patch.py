from pathlib import Path

path = Path('synctv-api-common/src/impls/client/convert.rs')
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    if old not in text:
        raise SystemExit(f'expected {label} anchor not found')
    text = text.replace(old, new, 1)


replace_once(
    '''        synctv_core::models::MediaSourceConfig::Youtube(config) => {
            Provider::Youtube(source_config_proto::YoutubeMediaSourceConfig {
                video_id: config.video_id,
                shared: config.shared,
            })
        }
        synctv_core::models::MediaSourceConfig::Huya(config) => {
''',
    '''        synctv_core::models::MediaSourceConfig::Youtube(config) => {
            Provider::Youtube(source_config_proto::YoutubeMediaSourceConfig {
                video_id: config.video_id,
                shared: config.shared,
            })
        }
        synctv_core::models::MediaSourceConfig::Iqiyi(config) => {
            Provider::Iqiyi(source_config_proto::IqiyiMediaSourceConfig {
                url: config.url,
                shared: config.shared,
                proxy_mode: playback_proxy_mode_to_proto(config.proxy_mode),
            })
        }
        synctv_core::models::MediaSourceConfig::TencentVideo(config) => {
            Provider::TencentVideo(source_config_proto::TencentVideoMediaSourceConfig {
                url: config.url,
                shared: config.shared,
                proxy_mode: playback_proxy_mode_to_proto(config.proxy_mode),
            })
        }
        synctv_core::models::MediaSourceConfig::Huya(config) => {
''',
    'media source proto conversion',
)

replace_once(
    '''        synctv_core::models::MediaSourceConfig::Youtube(config) => {
            format!("https://www.youtube.com/watch?v={}", config.video_id)
        }
        synctv_core::models::MediaSourceConfig::Huya(config) => match config {
''',
    '''        synctv_core::models::MediaSourceConfig::Youtube(config) => {
            format!("https://www.youtube.com/watch?v={}", config.video_id)
        }
        synctv_core::models::MediaSourceConfig::Iqiyi(config) => config.url.clone(),
        synctv_core::models::MediaSourceConfig::TencentVideo(config) => config.url.clone(),
        synctv_core::models::MediaSourceConfig::Huya(config) => match config {
''',
    'media resource metadata source',
)

path.write_text(text)
