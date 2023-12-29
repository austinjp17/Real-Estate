use serde_json::Value;
use scraper::{Html, Selector, ElementRef};
use polars::prelude::*;
use tracing::{info, trace, warn};
use std::{collections::VecDeque, fs::File};
use chrono::{Local, DateTime};


#[derive(Debug, Clone)]
pub(crate) enum ExtractionError {
    Price(String),
    Address(String)
}

#[derive(Debug, Clone)]
pub(crate) struct HomeAddress {
    street: String,
    apt: i32,
    city: String,
    state: String,
    zip: u32,//[u8; 5],
}

impl Into<String> for HomeAddress {
    fn into(self) -> String {
        match self.apt {
            -1 => format!("{}, {}, {} {}", self.street, self.city, self.state, self.zip),
            _ => {
                warn!("Formatting address with apt to string");
                format!("{}, {}, {}, {} {}", self.street, self.apt, self.city, self.state, self.zip)
            }
        }
        
    }
}

// Define a struct to represent historical price data.
#[derive(Debug)]
struct PriceHistory {
    price: u32,
    date: DateTime<Local>,
}

impl PriceHistory {
    fn new(price: u32, date: DateTime<Local>) -> Self {
        PriceHistory {
            price,
            date
        }
    }
}

impl Into<(DateTime<Local>, u32)> for PriceHistory {
    fn into(self) -> (DateTime<Local>, u32) {
        (self.date, self.price)
    }
}

#[derive(Debug)]
pub(crate) struct HomeListing {
    current_price: u32,
    beds: i32,
    baths: i32,
    sqft: u32,
    lot_size: i32,
    address: HomeAddress,
    price_history: Vec<PriceHistory>
}

impl HomeListing {
    
    /// Takes parsed HTML from a redfin listing and extracts key elements
    /// 
    /// Currently only returns error if Price is not found
    /// Sets null values if any of category is not found
    pub(crate) fn new_from_redfin(home_elem: &ElementRef) -> Result<Self, ExtractionError> {
        // extract price
        let current_price = extract_redfin_price(&home_elem)?;
        let date = Local::now();
        let price_history = vec![PriceHistory::new(current_price, date)];

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
            price_history,
        })
        
        
    }
}


pub(crate) struct ListingsContainer {
    pub(crate) queue: Vec<HomeListing>, // replace w/ Multiproducer single consumer??
    pub(crate) data: DataFrame,
    pub(crate) last_update: Option<DateTime<Local>>
}

impl Default for ListingsContainer {
    fn default() -> Self {
        // TODO: Schema instead ??
        
        Self { 
            queue: vec![], 
            data: DataFrame::empty(),
            last_update: None
        }
    }
}

impl ListingsContainer {
    /// Empty, no columns, dataframe
    pub(crate) fn new(queue: Vec<HomeListing>, data: DataFrame) -> Self {
        ListingsContainer { queue, data, last_update: None }
    }

    pub(crate) fn enqueue(&mut self, new_listings: &mut Vec<HomeListing>) {
        let expected_queue_len = self.queue.len() + new_listings.len();

        info!("Adding {} listings to queue", new_listings.len());
        self.queue.append(new_listings);

        // Assert items added
        assert_eq!(self.queue.len(), expected_queue_len);
        
    }

    /// Adds all listing objects in queue to data as new rows
    /// 
    /// empties queue
    pub(crate) fn handle_queue(&mut self) {
        let mut prices = vec![];
        let mut beds = vec![];
        let mut baths = vec![];
        let mut sqft = vec![];
        let mut lot_size = vec![];
        // Address Components
        let mut street = vec![];
        let mut apt = vec![];
        let mut city = vec![];
        let mut state = vec![];
        let mut zip = vec![];
        let mut addr_str: Vec<String> = vec![];
        
        // Order doesn't matter, can be parrelized
        self.queue.iter().for_each(|listing| {
            prices.push(listing.current_price);
            beds.push(listing.beds);
            baths.push(listing.baths);
            sqft.push(listing.sqft);
            lot_size.push(listing.lot_size);
            street.push(listing.address.street.clone());
            apt.push(listing.address.apt);
            city.push(listing.address.city.clone());
            state.push(listing.address.state.clone());
            zip.push(listing.address.zip.clone());
            // TODO: FIX
            // Clones entire object, then consumes clone to create string
            addr_str.push(listing.address.clone().into()); 
        });

        // All vecs same len
        assert!(prices.len() == beds.len() && prices.len() == baths.len() && prices.len() == sqft.len() && prices.len() == lot_size.len());
        
        let prices = Series::new("price", prices);
        let beds = Series::new("beds", beds);
        let baths = Series::new("baths", baths);
        let sqft = Series::new("sqft", sqft);
        let lot_size = Series::new("lot_size", lot_size);
        let street = Series::new("street", street);
        let apt = Series::new("apt", apt);
        let city = Series::new("city", city);
        let state = Series::new("state", state);
        let zip = Series::new("zip", zip);
        let addr_str = Series::new("addr_str", addr_str);

        let cols = vec![prices, beds, baths, sqft, lot_size, street, apt, city, state, zip, addr_str];
        let new_listings_df = DataFrame::new(cols).unwrap();

        // Add rows to dataframe
        self.data = self.data.vstack(&new_listings_df).expect("failed to concat new listings");

        // Clear Queue
        self.queue.clear();
        assert!(self.queue.is_empty());

    }

    pub(crate) fn to_csv(&mut self, path: &str) {
        let mut file = File::create(path).expect("file creation failed");

        if CsvWriter::new(&mut file)
            .finish(&mut self.data).is_err() {
                warn!("Error writing to csv");
            }
        
    }

    pub(crate) fn print_data_head(&self) {
        println!("{:?}", self.data.head(None))
    }
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

pub(crate) fn extract_redfin_address_str(home_elem: &ElementRef) -> Result<String, ExtractionError> {
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

