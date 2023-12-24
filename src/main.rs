use std::collections::VecDeque;

use anyhow::Result;
use tracing::{info, trace, warn};
use tracing_subscriber;
use scraper::{Html, Selector, ElementRef};
use serde_json::Value;

#[tracing::instrument]
fn redfin_url_builder() -> String {
    let mut base_url = String::from("https://www.redfin.com/");
    
    let suffix = "zipcode/77532";

    base_url.push_str(suffix);

    info!("Built Url: {}", base_url);

    base_url
}

/// Sends request to ScraperAPI for target_url
/// Returns html str
async fn request(target_url: &str) -> Result<String> {

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

    Ok(response_str)
}

/// contains text: "Viewing page x of n" in page control div
/// extracts, parses, and returns n
fn get_page_count(parsed_html: &Html) -> u8 {
    
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


struct HomeAddress {
    street: String,
    apt: Option<u16>,
    city: String,
    state: String,
    zip: Vec<u8>,//[u8; 5],
}
struct HomeListing {
    price: u32,
    beds: u8,
    baths: u8,
    sqft: u32,
    lot: Option<u32>,
    address: HomeAddress
}

impl HomeListing {
    pub fn from_redfin(home_elem: &ElementRef) -> Self {
        // extract price
        let price_id = r#"span[class="homecardV2Price"]"#;
        let price_sel = Selector::parse(price_id).unwrap();
        let price_str = home_elem.select(&price_sel).next().unwrap().inner_html();
        let price: u32 = (&price_str[1..]).replace(',', "").parse().unwrap();

        // Get Stats (beds, baths, sqftage, lot size)
        let stats_id = r#"div[class="stats"]"#;
        let stats_sel = Selector::parse(stats_id).unwrap();
        let stat_elems = home_elem.select(&stats_sel);

        
        let mut beds: u8 = u8::MAX;
        let mut baths = u8::MAX;
        let mut sqft = u32::MAX;
        let mut lot: Option<u32> = None;
        for e in stat_elems {
            let stat_str = e.inner_html();
            if stat_str.contains("beds") {
                beds = stat_str.chars().next().unwrap().to_digit(10).unwrap() as u8;
            }

            else if stat_str.contains("baths") {
                baths = stat_str.chars().next().unwrap().to_digit(10).unwrap() as u8;
            }

            // Lot stat will be in sqft or acreage
            // may contain "sq ft"
            // check for lot size before house sq ft
            else if stat_str.contains("lot") {
                let split_items:Vec<&str> = stat_str.split(" ").collect();
                
                // if len split is 4 then lot measured in sqft (Desired)
                if split_items.len() == 4 {
                    lot = Some(split_items.first().unwrap().replace(",", "").parse::<u32>().expect("failed to parse"));
                }

                // if 3, then measured in acreage
                else if split_items.len() == 3 {

                    fn acre_to_sqft(acres: f32) -> u32 {
                        return (acres * 43460_f32) as u32;
                    }

                    let lot_acres = split_items.first().unwrap().parse::<f32>().expect("failed to parse");
                    lot = Some(acre_to_sqft(lot_acres));

                }

                else { warn!("Unexpected number of items in stat string: {:?}", split_items); }
            
            }

            // won't be reached on lots measured in sqftage
            else if stat_str.contains("sq ft") {
                let split_items:Vec<&str> = stat_str.split(" ").collect();
                if split_items.len() != 3 {
                    println!("{:?}", split_items);
                    assert_eq!(split_items.len(), 3); // Should be [target_num, "sq", "ft"]
                }

                
                
                sqft = split_items.first().unwrap().replace(",", "").parse().unwrap();
            }

            

            else {
                warn!("Unrecognized stat: {}", stat_str);
            }
        }
        

        // Get Address
        let address_id = r#"span[class="collapsedAddress primaryLine"]"#;
        let address_sel = Selector::parse(address_id).unwrap();
        let address_str = home_elem.select(&address_sel).next().unwrap().inner_html();
        

        // Initally 3 compenents: [street, city, (state zip)]
        let mut addr_components = address_str.split(",").map(|a| a.trim().to_string()).collect::<VecDeque<String>>();
        // split state and zip
        let mut zip_state_expansion = addr_components.pop_back().unwrap().split(" ").map(|s| s.to_string()).collect::<VecDeque<String>>(); 
        addr_components.append(&mut zip_state_expansion);


        // Build Address Object
        // remove & save apt component if present
        let apt = if addr_components.len() == 4 {
            None
        } else {
            Some(addr_components.remove(2).unwrap().parse().unwrap())
        };
        
        assert_eq!(addr_components.len(), 4); // [street, city, state, zip]
        let street = addr_components.pop_front().unwrap();
        let city = addr_components.pop_front().unwrap();
        let state = addr_components.pop_front().unwrap();
        let zip = addr_components.pop_front().unwrap().chars().collect::<Vec<char>>().into_iter().map(|c| c.to_digit(10).unwrap() as u8).collect::<Vec<u8>>();

        assert_eq!(state.chars().collect::<Vec<char>>().len(), 2); // State should always be two letters
        assert_eq!(zip.len(), 5); // Zip should always be 5 digits
        assert!(street.chars().collect::<Vec<char>>().len() > city.chars().collect::<Vec<char>>().len());

        let addr_obj = HomeAddress {
            street,
            apt,
            city,
            state,
            zip,
        };
        // println!("Price: {}", price);

        assert_ne!(beds, u8::MAX);
        assert_ne!(baths, u8::MAX);
        assert_ne!(sqft, u32::MAX);
        HomeListing {
            price,
            beds,
            baths,
            sqft,
            lot,
            address: addr_obj,
        }
        
        
    }
}



fn get_page_homes(parsed_html: &Html) {
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
}


#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let url = redfin_url_builder();
    let request_result = request(&url).await;
    if request_result.is_err() { 
        let e = request_result.expect_err("Conditioned for");
        warn!("Request Error: {}", &e);
        panic!("Request Error: {}", e); 
    }

    let response = request_result.expect("conditioned");
    let parsed = Html::parse_document(&response);

    let page_count = get_page_count(&parsed);

    get_page_homes(&parsed);

    

    // println!("{:?}", elems);

    
}
