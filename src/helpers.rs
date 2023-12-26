use scraper::Html;
use anyhow::Result;
use tracing::{info, trace, warn};


pub(crate) async fn request(target_url: &str) -> Result<Html> {

    // Required ScraperAPI request params
    let params = [
        ("url", target_url),
        ("api_key", "0861bae719981ddf7ae64ddfcb5193ad")
    ];

    let scraper_url = "https://api.scraperapi.com/";
    let scraper_url = reqwest::Url::parse_with_params(scraper_url, params).unwrap();

    // Send Request
    let response = reqwest::get(scraper_url).await?;
    info!("Response Code: {}", response.status());

    // Convert resp to HTML str
    let response_str = response.text().await?;

    let parsed = Html::parse_document(&response_str);

    Ok(parsed)
}