use anyhow::Result;
use reqwest::{Client, header::{HeaderMap, HeaderValue, COOKIE}};
use tracing::info;
use tracing_subscriber;
use scraper::{Html, Selector, Element};

#[tracing::instrument]
fn redfin_url_builder() -> String {
    let mut base_url = String::from("https://www.redfin.com/");
    
    let suffix = "zipcode/77532";

    base_url.push_str(suffix);

    info!("Built Url: {}", base_url);

    base_url
}

async fn request(target_url: &str) -> Result<String> {

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
    let response = reqwest::get(url).await?;
    info!("Response Code: {}", response.status());
    let response_str = response.text().await?;


    Ok(response_str)
}

/// contains text: "Viewing page x of n" in page control div
/// extracts, parses, and returns n
fn get_page_count(parsed_html: &Html) -> u8 {
    // Find page controls container
    
    let page_count_span = r#"span[class="pageText"]"#;
    let page_count_selector = Selector::parse(page_count_span).unwrap();

    let page_count_container = parsed_html.select(&page_count_selector).next().unwrap();
    let page_count_str: String = page_count_container.inner_html();
    let page_count: u8 = page_count_str.chars().last().unwrap()
        .to_digit(10).unwrap().try_into().unwrap();
    
    info!("Found {} pages", page_count);
    page_count
}



#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let url = redfin_url_builder();
    let request_result = request(&url).await;
    if request_result.is_err() { 
        panic!("Request Error: {}", request_result.expect_err("Conditioned for") ) 
    }

    let response = request_result.expect("conditioned");
    let parsed = Html::parse_document(&response);

    let page_count = get_page_count(&parsed);

    let home_card_div = r#"div[class="HomeCardContainer defaultSplitMapListView"]"#;
    let home_selector = Selector::parse(home_card_div).unwrap();
    
    
    
    let mut i = 0;
    // for element in page_controls.select(&Selector::parse("div").unwrap()) {
    //     println!("{:?}", i);
    //     println!("Element: {:?}", element.value());
    //     println!("Element Child: {:?}", element.children());
    //     i += 1;
    // }

    // println!("{:?}", elems);

    
}
