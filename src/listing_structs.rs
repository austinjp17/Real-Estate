use serde_json::Value;
use scraper::{Html, Selector, ElementRef};
use polars::prelude::*;
use tracing::{info, trace, warn};
use std::{collections::VecDeque, fs::File};

pub(crate) struct HomeAddress {
    street: String,
    apt: i32,
    city: String,
    state: String,
    zip: u32,//[u8; 5],
}

pub(crate) struct HomeListing {
    price: u32,
    beds: u8,
    baths: u8,
    sqft: u32,
    lot_size: i32,
    address: HomeAddress
}

impl HomeListing {
    /// Takes parsed HTML from a redfin listing and extracts key elements
    pub(crate) fn from_redfin(home_elem: &ElementRef) -> Self {
        // extract price
        let price_id = r#"span[class="homecardV2Price"]"#;
        let price_sel = Selector::parse(price_id).unwrap();
        let price_str = home_elem.select(&price_sel).next().unwrap().inner_html();
        let price: u32 = (&price_str[1..]).replace(',', "").parse().unwrap();

        // Get Stats (beds, baths, sqftage, lot size)
        let stats_id = r#"div[class="stats"]"#;
        let stats_sel = Selector::parse(stats_id).unwrap();
        let stat_elems = home_elem.select(&stats_sel);

        // Parse stats
        let mut beds: u8 = u8::MAX;
        let mut baths = u8::MAX;
        let mut sqft = u32::MAX;
        let mut lot_size = -1_i32;
        for e in stat_elems {
            let stat_str = e.inner_html();
            // Number of bedrooms
            if stat_str.contains("beds") {
                beds = stat_str.chars().next().unwrap().to_digit(10).unwrap() as u8;
            }
            // Number of Bathrooms
            else if stat_str.contains("baths") {
                baths = stat_str.chars().next().unwrap().to_digit(10).unwrap() as u8;
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
            else if stat_str.contains("sq ft") {
                let split_items:Vec<&str> = stat_str.split(" ").collect();
                if split_items.len() != 3 {
                    println!("{:?}", split_items);
                    assert_eq!(split_items.len(), 3); // Should be [target_num, "sq", "ft"]
                }
                
                sqft = split_items.first().unwrap().replace(",", "").parse().unwrap();
            }

            else { warn!("Unrecognized stat: {}", stat_str); }
        }
        

        // Get Address
        let address_id = r#"span[class="collapsedAddress primaryLine"]"#;
        let address_sel = Selector::parse(address_id).unwrap();
        let address_str = home_elem.select(&address_sel).next().unwrap().inner_html();
        
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
            info!("Apartment found");
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

        let addr_obj = HomeAddress {
            street,
            apt,
            city,
            state,
            zip,
        };
        
        assert_ne!(beds, u8::MAX);
        assert_ne!(baths, u8::MAX);
        assert_ne!(sqft, u32::MAX);
        assert_ne!(sqft as i32, lot_size);
        trace!("Redfin Listing extracted");

        HomeListing {
            price,
            beds,
            baths,
            sqft,
            lot_size,
            address: addr_obj,
        }
        
        
    }

}


pub(crate) struct ListingsContainer {
    pub(crate) queue: Vec<HomeListing>, // replace w/ Multiproducer single consumer??
    pub(crate) data: DataFrame,
}

impl Default for ListingsContainer {
    fn default() -> Self {
        let price_col = Series::new_empty("price", &DataType::UInt32);
        let beds_col = Series::new_empty("beds", &DataType::UInt8);
        let baths_col = Series::new_empty("baths", &DataType::UInt8);
        let sqft_col = Series::new_empty("sqft", &DataType::UInt32);
        let lot_size_col = Series::new_empty("lot_size", &DataType::Int32);
        let street_col = Series::new_empty("street", &DataType::Utf8);
        let apt_col = Series::new_empty("apt", &DataType::Int32);
        let city_col = Series::new_empty("city", &DataType::Utf8);
        let state_col = Series::new_empty("state", &DataType::Utf8);
        let zip_col = Series::new_empty("zip", &DataType::UInt32);
        let cols = vec![price_col, beds_col, baths_col, sqft_col, lot_size_col, street_col, apt_col, city_col, state_col, zip_col];
        Self { queue: vec![], data: DataFrame::new(cols).expect("failed to create default df") }
    }
}

impl ListingsContainer {
    pub(crate) fn new(queue: Vec<HomeListing>, data: DataFrame) -> Self {
        ListingsContainer { queue, data }
    }

    pub(crate) fn enqueue(&mut self, new_listings: &mut Vec<HomeListing>) {
        let expected_queue_len = self.queue.len() + new_listings.len();

        info!("Adding {} listings to queue", new_listings.len());
        self.queue.append(new_listings);

        // Assert items added
        assert_eq!(self.queue.len(), expected_queue_len);
        
    }
    /// Adds all listing objects in queue to data as new rows
    /// empties queue
    /// TODO: Handle Address
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
        // let mut address = vec![];
        
        // Order doesn't matter, can be parrelized
        self.queue.iter().for_each(|listing| {
            prices.push(listing.price);
            beds.push(listing.beds);
            baths.push(listing.baths);
            sqft.push(listing.sqft);
            lot_size.push(listing.lot_size);
            street.push(listing.address.street.clone());
            apt.push(listing.address.apt);
            city.push(listing.address.city.clone());
            state.push(listing.address.state.clone());
            zip.push(listing.address.zip.clone())
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

        let cols = vec![prices, beds, baths, sqft, lot_size, street, apt, city, state, zip];
        let new_listings_df = DataFrame::new(cols).unwrap();

        // Add rows to dataframe
        self.data = self.data.vstack(&new_listings_df).expect("failed to concat new listings");

        // Clear Queue
        self.queue.clear();
        assert!(self.queue.is_empty());

    }

    pub(crate) fn to_csv(&mut self, path: &str) {
        let mut file = File::create(path).expect("asdf");

        if CsvWriter::new(&mut file)
            .finish(&mut self.data).is_err() {
                warn!("Error writing to csv");
            }
        
    }
}
