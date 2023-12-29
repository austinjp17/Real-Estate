use scraper::{Html, Selector, ElementRef};
use tracing::{info, trace, warn};
use tracing_subscriber;
use polars::prelude::*;
use crate::{listing_structs::{HomeListing, ListingsContainer, extract_redfin_address_str}, helpers};


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


impl ListingsContainer {
    
    pub(crate) fn house_exisits_in_dataset(&self, home_elem: &ElementRef) -> bool {
        let addr_str = extract_redfin_address_str(home_elem).expect("address failed to extract");
        match self.data.clone().lazy().filter(
            col("addr_str").eq(lit(addr_str))
        ).collect().unwrap().is_empty() {
            true => false,
            false => true
        }

        
    }

    /// Gets all home listings from a redfin page and adds them as 'HomeListing' objects
    /// to self.queue
    pub(crate) fn parse_redfin_page(&mut self, parsed_html: &Html) {
        let mut listings: Vec<HomeListing> = vec![];

        let unfocused_home_card_div = r#"div[class="HomeCardContainer defaultSplitMapListView"]"#;
        let unfocused_home_selector = Selector::parse(unfocused_home_card_div).unwrap();

        let focused_home_card_div = r#"div[class="HomeCardContainer selectedHomeCard defaultSplitMapListView"]"#;
        let focused_home_selector = Selector::parse(focused_home_card_div).unwrap();

        let mut focused_home = parsed_html.select(&focused_home_selector);
        let unfocused_homes = parsed_html.select(&unfocused_home_selector);

        let focused_home = focused_home.next().unwrap();
        match self.house_exisits_in_dataset(&focused_home) {
            true => todo!(), // scrape price and add to price history vec
            false => {
                if let Ok(listing) = HomeListing::new_from_redfin(&focused_home) {
                    self.queue.push(listing)
                }
            }
        };
        

        // Start from 1 b/c focused home uncounted
        let mut i = 1;
        for home_elem in unfocused_homes {
            // If home already exists in dataset
            match self.house_exisits_in_dataset(&home_elem) {
                true => todo!(), // scrape price and add to price history vec

                // Create new row entry
                false => {
                    let extraction_res = HomeListing::new_from_redfin(&home_elem);
                    match extraction_res.is_ok() {
                        true => listings.push(extraction_res.unwrap()),
                        false => warn!("Skipping listing: {:?}", extraction_res.unwrap_err())
                    }
                }
            }
            i += 1;
        }

        info!("Number of houses on page found: {}", i);
    }
    
    /// Gets all redfin home listings for a given zipcode
    /// 
    /// Calls parse_redfin_page on all found pages then handles all elements in self.queue
    pub(crate) async fn homes_by_zip(&mut self, zipcode: u32) {

        // First run gets number of pages
        let url = url_builder(SearchBy::Zipcode, zipcode, None);
        let request_result = helpers::request(&url).await;
        if request_result.is_err() { 
            let e = request_result.unwrap_err();
            warn!("Request Error: {}", &e);
            panic!("Request Error: {}", e); 
        }

        let response = request_result.expect("conditioned");
        
        let page_count = get_redfin_page_count(&response);
        self.parse_redfin_page(&response);

        for page_num in 2..=page_count {
            let url = url_builder(SearchBy::Zipcode, zipcode, Some(page_num));
            let request_result = helpers::request(&url).await;
            if request_result.is_err() { 
                let e = request_result.expect_err("Conditioned for");
                warn!("Request Error: {}", &e);
                panic!("Request Error: {}", e); 
            }
            let response = request_result.expect("conditioned");
            self.parse_redfin_page(&response);
        };

        
        self.handle_queue();

    }

    

}


pub(crate) fn get_redfin_page_count(parsed_html: &Html) -> u8 {
    
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