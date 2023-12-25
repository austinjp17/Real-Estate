use scraper::{Html, Selector};
use tracing::{info, trace, warn};
use tracing_subscriber;
use polars::prelude::*;
use crate::listing_structs::HomeListing;

#[tracing::instrument]
pub(crate) fn url_builder() -> String {
    let mut base_url = String::from("https://www.redfin.com/");
    
    let suffix = "zipcode/77532";

    base_url.push_str(suffix);

    info!("Built Url: {}", base_url);

    base_url
}


/// contains text: "Viewing page x of n" in page control div
/// extracts, parses, and returns n
#[tracing::instrument]
pub(crate) fn get_page_count(parsed_html: &Html) -> u8 {
    
    // Define Target Span & build html selector
    let page_count_span = r#"span[class="pageText"]"#;
    let page_count_selector = Selector::parse(page_count_span).unwrap();

    // Find target span
    let page_count_container = parsed_html.select(&page_count_selector).next().unwrap();
    
    // Get target inner html
    let page_count_str: String = page_count_container.inner_html();
    
    // take last char representing n and parse
    let page_count: u8 = page_count_str.chars().last().unwrap()
        .to_digit(10).unwrap().try_into().unwrap();
    
    info!("Number of pages found: {}", page_count);
    page_count
}

pub(crate) fn get_page_homes(parsed_html: &Html) -> Vec<HomeListing> {
    let mut listings: Vec<HomeListing> = vec![];

    let unfocused_home_card_div = r#"div[class="HomeCardContainer defaultSplitMapListView"]"#;
    let unfocused_home_selector = Selector::parse(unfocused_home_card_div).unwrap();

    let focused_home_card_div = r#"div[class="HomeCardContainer selectedHomeCard defaultSplitMapListView"]"#;
    let focused_home_selector = Selector::parse(focused_home_card_div).unwrap();

    let mut focused_home = parsed_html.select(&focused_home_selector);
    let unfocused_homes = parsed_html.select(&unfocused_home_selector);

    let focused_home = focused_home.next().unwrap();
    listings.push(HomeListing::from_redfin(&focused_home));

    // Start from 1 b/c focused home uncounted
    let mut i = 1;
    for home_elem in unfocused_homes {
        listings.push(HomeListing::from_redfin(&home_elem));
        i += 1;
    }

    info!("Number of houses found: {}", i);

    listings
}

fn get_all_pages_data(page_count: u8) {

}