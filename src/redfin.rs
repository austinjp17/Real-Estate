use scraper::{Html, Selector};
use tracing::{info, trace, warn};
use tracing_subscriber;
use polars::prelude::*;
use crate::{listing_structs::{HomeListing, ListingsContainer}, helpers};

#[derive(Debug, Copy, Clone)]
enum SearchBy {
    City,
    Address,
    School,
    Agent,
    Zipcode
}

#[tracing::instrument]
// Target taken as u32 only currently b/c zipcode only search used
pub(crate) fn url_builder(search_category: SearchBy, search_target: u32, page_num: Option<u8>) -> String {
    let mut base_url = String::from("https://www.redfin.com/");
    
    let target_query = match search_category {
        SearchBy::Zipcode => format!("zipcode/{}", search_target), // ONLY USE THIS ONE
        SearchBy::Address => format!("address/{}", search_target), // WRONG BUT FIXABLE
        SearchBy::City => format!("city/randnum/TX/{}", search_target), // WRONG idk randnum
        SearchBy::Agent => format!("Idk what this is"),
        SearchBy::School => format!("don'tuse"),
        
    };
    base_url = format!("{}{}", base_url, target_query);

    let page_query = match page_num {
        None => String::from(""),
        Some(n) => {
            format!("/page-{}", n)
        }
    };
    base_url = format!("{}{}", base_url, page_query);

    

    info!("Built Url: {}", base_url);

    base_url
}


/// contains text: "Viewing page x of n" in page control div
/// extracts, parses, and returns n

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
    
    if let Ok(listing) = HomeListing::from_redfin(&focused_home) {
        listings.push(listing)
    }

    // Start from 1 b/c focused home uncounted
    let mut i = 1;
    for home_elem in unfocused_homes {
        let extraction_res = HomeListing::from_redfin(&home_elem);
        match extraction_res.is_ok() {
            true => listings.push(extraction_res.unwrap()),
            false => warn!("Skipping listing: {:?}", extraction_res.unwrap_err())
        }
        i += 1;
    }

    info!("Number of houses on page found: {}", i);

    listings
}

pub(crate) async fn homes_by_zip(zipcode: u32) -> ListingsContainer {
    // Holds all home listings
    let mut listings_container = ListingsContainer::default();

    // First run gets number of pages
    let url = url_builder(SearchBy::Zipcode, zipcode, None);
    let request_result = helpers::request(&url).await;
    if request_result.is_err() { 
        let e = request_result.expect_err("Conditioned for");
        warn!("Request Error: {}", &e);
        panic!("Request Error: {}", e); 
    }

    let response = request_result.expect("conditioned");
    
    let page_count = get_page_count(&response);
    let mut new_listings = get_page_homes(&response);
    listings_container.enqueue(&mut new_listings);
    listings_container.handle_queue();

    for page_num in 2..=page_count {
        let url = url_builder(SearchBy::Zipcode, zipcode, Some(page_num));
        let mut new_listings = fetch_and_process(&url).await;
        listings_container.enqueue(&mut new_listings);
        listings_container.handle_queue();
    };

    listings_container
}

async fn fetch_and_process(url: &str) -> Vec<HomeListing> {
    let request_result = helpers::request(url).await;
    if request_result.is_err() { 
        let e = request_result.expect_err("Conditioned for");
        warn!("Request Error: {}", &e);
        panic!("Request Error: {}", e); 
    }
    let response = request_result.expect("conditioned");
    let mut new_listings = get_page_homes(&response);
    new_listings
    // listings_container.enqueue(&mut new_listings);
    // listings_container.handle_queue();
}