use scraper::{Html, Selector, ElementRef};
use tracing::{info, trace, warn};
use tracing_subscriber;
use polars::prelude::*;
use crate::{listing_structs::{PriceHistory, HomeAddress, HomeListing, ListingsContainer, ExtractionError}, helpers};
use chrono::{Local, DateTime};
use std::collections::VecDeque;

#[derive(Debug, Copy, Clone)]
enum SearchBy {
    City,
    Address,
    School,
    Agent,
    Zipcode
}




impl HomeListing {
    
    /// Takes parsed HTML from a redfin listing and extracts key elements
    /// 
    /// Currently only returns error if Price is not found
    /// Sets null values if any of category is not found
    pub(crate) fn new_from_redfin(home_elem: &ElementRef) -> Result<Self, ExtractionError> {
        // extract price
        let current_price = extract_redfin_price(&home_elem)?;
        // let date = Local::now();
        // let price_history = vec![PriceHistory::new(current_price, date)];

        // Get Stats (beds, baths, sqftage, lot size)
        let stats_id = r#"div[class="stats"]"#;
        let stats_sel = Selector::parse(stats_id).unwrap();
        let stat_elems = home_elem.select(&stats_sel);

        // Parse stats
        let mut beds = i32::MAX;
        let mut baths = i32::MAX;
        let mut sqft = u32::MAX;
        let mut lot_size = -1_i32;
        for e in stat_elems {
            let stat_str = e.inner_html();
            // Number of bedrooms
            if stat_str.contains("bed") || stat_str.contains("beds") {
                let beds_res = stat_str.chars().next().unwrap().to_digit(10);
                beds = match beds_res {
                    None => -1,
                    Some(num) => num as i32
                }
            }
            // Number of Bathrooms
            else if stat_str.contains("bath") || stat_str.contains("baths") {
                let baths_res = stat_str.chars().next().unwrap().to_digit(10);
                baths = match baths_res {
                    None => -1,
                    Some(num) => num as i32
                }
            }

            // Lot Size
            // Lot stat will be in sqft or acreage
            // may contain "sq ft"
            // check for lot size before house sq ft
            else if stat_str.contains("lot") {
                let split_items:Vec<&str> = stat_str.split(" ").collect();
                
                // if len split is 4 then lot measured in sqft (Desired)
                if split_items.len() == 4 {
                    lot_size = split_items.first().unwrap().replace(",", "").parse::<i32>().expect("failed to parse");
                }

                // if 3, then measured in acreage
                else if split_items.len() == 3 {

                    fn acre_to_sqft(acres: f32) -> i32 { (acres * 43460_f32) as i32 }

                    let lot_acres = split_items.first().unwrap().parse::<f32>().expect("failed to parse");
                    lot_size = acre_to_sqft(lot_acres);

                }

                else { warn!("Unexpected number of items in stat string: {:?}", split_items); }
            
            }
            
            // House Sqftage
            // won't be reached on lots measured in sqftage
            // Can sometimes be null if house is being constructed
            else if stat_str.contains("sq ft") {
                let split_items:Vec<&str> = stat_str.split(" ").collect();
                if split_items.len() != 3 {
                    println!("{:?}", split_items);
                    assert_eq!(split_items.len(), 3); // Should be [target_num, "sq", "ft"]
                }
                
                let sqft_res: Result<u32, _> = split_items.first().unwrap().replace(",", "").parse();
                sqft = match sqft_res.is_err() {
                    true => 0,
                    false => sqft_res.unwrap()
                }
            }

            else { warn!("Unrecognized stat: {}", stat_str); }
        }

        // unset sqft means no house, just lot
        if sqft == u32::MAX {
            sqft = 0;
        }
        
        // Get Address
        let addr_str = extract_redfin_address_str(home_elem)?;
        let addr_obj = parse_redfin_address_str(home_elem)?;
        
        // Checks
        assert_ne!(beds, i32::MAX);
        assert_ne!(baths, i32::MAX);
        assert_ne!(sqft, u32::MAX);
        assert_ne!(sqft as i32, lot_size);
        trace!("Redfin Listing extracted");

        Ok(HomeListing {
            current_price,
            beds,
            baths,
            sqft,
            lot_size,
            address: addr_obj,
        })
        
        
    }

}


impl ListingsContainer {
    
    //TODO: NOT WORKING! RETURNS TRUE FOR ALL
    pub(crate) fn house_exisits_in_dataset(&self, home_elem: &ElementRef) -> bool {
        let addr_str = extract_redfin_address_str(home_elem).expect("address failed to extract");
        
        self.listing_features.clone()
            .lazy()
            // filter for rows w/ address
            .filter(col("addr_str").eq(lit(addr_str)))
            // Count rows
            .select([count().alias("count")])
            .collect().expect("created above")
            .column("count").expect("col created here")
            .u32().expect("count col always numeric")
            .get(0).expect("count col always has single row") > 0
            
        
    }
            

    // TODO: Don't add if within same day
    pub(crate) fn update_existing_redfin(&mut self, home_elem: &ElementRef) -> Result<(), ExtractionError> {
        let addr_str = extract_redfin_address_str(&home_elem).expect("address found in house_exists fn()");
        let curr_price = extract_redfin_price(&home_elem).expect("address already found");
        let unix_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32; // Should be u32????

        let addr_str = Series::new("addr_str", vec![addr_str]);
        let price = Series::new("price", vec![curr_price]);
        let date = Series::new("date", vec![unix_time]);
        
        let new_row = DataFrame::new(vec![addr_str, date, price]).expect("Failed to create update dateframe"); // I64 -> u32?????
        self.listing_history = self.listing_history.vstack(&new_row).expect("Failed to update listing");
        
        Ok(())
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

        // Check if house exists in dataset or if forced entry refresh
        if !self.force_refresh && self.house_exisits_in_dataset(&focused_home) {
            // scrape price and add to price history dataset but not listing dataset
            if let Err(e) = self.update_existing_redfin(&focused_home) {
                warn!("Failed to update: {:?}", e);
            }
            
        }
        // House not found in dataset
        // Add to features && price datasets
        else {
            if let Ok(listing) = HomeListing::new_from_redfin(&focused_home) {
                self.queue.push(listing);
            }
        }
        
        

        // Start from 1 b/c focused home uncounted
        let mut i = 1;
        for home_elem in unfocused_homes {
            // If home already exists in dataset
            if !self.force_refresh && self.house_exisits_in_dataset(&home_elem) {
                if let Err(e) = self.update_existing_redfin(&home_elem) {
                    warn!("Failed to update: {:?}", e);
                }
            }
            // Create new row entry
            else {
                let listing_res = HomeListing::new_from_redfin(&home_elem);
                if let Err(e) =  listing_res {
                    warn!("Skipping Listing: {:?}", e);
                } 
                else { self.queue.push(listing_res.expect("conditioned")); }
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
        self.handle_queue();

        if !self.first_page_only {
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

    

}


// Extraction Helpers

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

fn extract_redfin_price(home_elem: &ElementRef) -> Result<u32, ExtractionError> { 
    let price_id = r#"span[class="homecardV2Price"]"#;
    let price_sel = Selector::parse(price_id).unwrap();
    let price_str = home_elem.select(&price_sel).next().unwrap().inner_html();
    let parsed_price = {
        let cleaned_price_str = (&price_str[1..]).replace(',', "");
        cleaned_price_str.parse::<u32>().map_err(|_| ExtractionError::Price(price_str))
    };

    parsed_price
}

fn extract_redfin_address_str(home_elem: &ElementRef) -> Result<String, ExtractionError> {
        // Get Address
        let address_id = r#"span[class="collapsedAddress primaryLine"]"#;
        let address_sel = Selector::parse(address_id).expect("valid above html");
        let address_str = home_elem.select(&address_sel).next().unwrap().inner_html(); // TODO: handle address not found error
        Ok(address_str)
        
    
}

fn parse_redfin_address_str(home_elem: &ElementRef) -> Result<HomeAddress, ExtractionError> {
    let address_str = extract_redfin_address_str(home_elem)?;
    // Parse address
    // Initally 3 compenents: [street, city, (state zip)]
    let mut addr_components = address_str.split(",").map(|a| a.trim().to_string()).collect::<VecDeque<String>>();
    // split state and zip
    let mut zip_state_expansion = addr_components.pop_back().unwrap().split(" ").map(|s| s.to_string()).collect::<VecDeque<String>>(); 
    addr_components.append(&mut zip_state_expansion);
    // Build Address Object
    // remove & save apt component if present
    let apt = if addr_components.len() == 4 {
        -1
    } else {
        warn!("Apartment found");
        addr_components.remove(2).unwrap().parse().unwrap()
    };
    
    assert_eq!(addr_components.len(), 4); // [street, city, state, zip]
    let street = addr_components.pop_front().unwrap();
    let city = addr_components.pop_front().unwrap();
    let state = addr_components.pop_front().unwrap();
    let zip: u32 = addr_components.pop_front().unwrap().parse().unwrap();

    // Address correctness assertions
    assert_eq!(state.chars().collect::<Vec<char>>().len(), 2); // State should always be two letters
    // assert_eq!(zip.len(), 5); // Zip should always be 5 digits
    assert!(street.chars().collect::<Vec<char>>().len() > city.chars().collect::<Vec<char>>().len());

    Ok(HomeAddress {
        street,
        apt,
        city,
        state,
        zip,
    })
}








