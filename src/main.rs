use std::collections::VecDeque;

use anyhow::Result;
use tracing::{info, trace, warn};
use tracing_subscriber;
use scraper::Html;


mod redfin;
mod listing_structs;



#[tracing::instrument]

/// Sends request to ScraperAPI for target_url
/// Returns html str
async fn request(target_url: &str) -> Result<Html> {

    // Required ScraperAPI request params
    let params = [
        ("url", target_url),
        ("api_key", "0861bae719981ddf7ae64ddfcb5193ad")
    ];

    let scraper_url = "https://api.scraperapi.com/";
    let scraper_url = reqwest::Url::parse_with_params(scraper_url, params).unwrap();

    // Send Request
    info!("Getting {}...", target_url);
    let response = reqwest::get(scraper_url).await?;
    info!("Response Code: {}", response.status());

    // Convert resp to HTML str
    let response_str = response.text().await?;

    let parsed = Html::parse_document(&response_str);

    Ok(parsed)
}

#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let url = redfin::url_builder();
    let request_result = request(&url).await;
    if request_result.is_err() { 
        let e = request_result.expect_err("Conditioned for");
        warn!("Request Error: {}", &e);
        panic!("Request Error: {}", e); 
    }

    let response = request_result.expect("conditioned");
    
    let page_count = redfin::get_page_count(&response);

    redfin::get_page_homes(&response);

    

    // println!("{:?}", elems);

    
}
