use anyhow::Result;
use reqwest::{Client, header::{HeaderMap, HeaderValue, COOKIE}};
use tracing::info;
use tracing_subscriber;

#[tracing::instrument]
fn redfin_url_builder() -> String {
    let mut base_url = String::from("https://www.redfin.com/");
    
    let suffix = "zipcode/77532";

    base_url.push_str(suffix);

    info!("Built Url: {}", base_url);

    base_url
}

async fn request(target_url: &str) -> Result<reqwest::Response> {

    let mut headers = HeaderMap::new();
    
    let base_headers = [
        "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0"
    ];

    let params = [
        ("url", target_url),
        ("api_key", "0861bae719981ddf7ae64ddfcb5193ad")
    ];

    let url = "https://api.scraperapi.com/";
    let url = reqwest::Url::parse_with_params(url, params).unwrap();

    for h in base_headers {
        headers.insert(COOKIE, HeaderValue::from_str(h).unwrap());
    }

    info!("Getting {}...", target_url);
    let body = reqwest::get(url).await?;
    info!("Response Code: {}", body.status());

    Ok(body)
}

#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let url = redfin_url_builder();
    let response = request(&url).await;
    
}
