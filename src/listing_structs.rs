use polars::prelude::*;
use tracing::{info, trace, warn};
use std::fs::File;
use chrono::{Local, DateTime};


#[derive(Debug, Clone)]
pub(crate) enum ExtractionError {
    Price(String),
    Address(String)
}

#[derive(Debug, Clone)]
pub(crate) struct HomeAddress {
    pub street: String,
    pub apt: i32,
    pub(crate) city: String,
    pub(crate) state: String,
    pub(crate) zip: u32,//[u8; 5],
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
pub(crate) struct PriceHistory {
    price: u32,
    date: DateTime<Local>,
}

impl PriceHistory {
    pub(crate) fn new(price: u32, date: DateTime<Local>) -> Self {
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
    pub(crate) current_price: u32,
    pub(crate) beds: i32,
    pub(crate) baths: i32,
    pub(crate) sqft: u32,
    pub(crate) lot_size: i32,
    pub(crate) address: HomeAddress,
}


pub(crate) struct ListingsContainer {
    pub(crate) queue: Vec<HomeListing>, // replace w/ Multiproducer single consumer??
    pub(crate) listing_features: DataFrame,
    pub(crate) listing_history: DataFrame,
    pub(crate) last_update: Option<DateTime<Local>>,
    pub(crate) force_refresh: bool,
}

impl Default for ListingsContainer {
    fn default() -> Self {
        // TODO: Schema instead ??
        
        Self { 
            queue: vec![], 
            listing_features: DataFrame::empty(),
            listing_history: DataFrame::empty(),
            last_update: None,
            force_refresh: false,
        }
    }
}

impl ListingsContainer {
    /// Empty, no columns, dataframe
    pub(crate) fn new(force_refresh: bool) -> Self {
        ListingsContainer { 
            queue: vec![], 
            listing_features: DataFrame::empty(),
            listing_history: DataFrame::empty(), 
            last_update: None,
            force_refresh }
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

        // Historical Components
        let mut prices = vec![];
        let mut dates = vec![];

        
        // Order doesn't matter, can be parrelized
        self.queue.iter().for_each(|listing| {
            
            // Features
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

            // Price
            prices.push(listing.current_price);
            let unix_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
            dates.push(unix_time);
            
        });
        

        // All vecs same len
        assert!(prices.len() == beds.len() && prices.len() == baths.len() && prices.len() == sqft.len() && prices.len() == lot_size.len());
        
        
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

        let prices = Series::new("price", prices);
        let dates = Series::new("date", dates);

        let feature_cols = vec![beds, baths, sqft, lot_size, street, apt, city, state, zip, addr_str.clone()];
        let history_cols = vec![addr_str, dates, prices];

        let new_listing_features_df = DataFrame::new(feature_cols).unwrap();

        let new_history_df = DataFrame::new(history_cols).unwrap();

        // Add rows to dataframe
        self.listing_features = self.listing_features.vstack(&new_listing_features_df).expect("failed to concat new listings");

        self.listing_history = self.listing_history.vstack(&new_history_df).expect("Failed to add price history rows");

        // Clear Queue
        self.queue.clear();
        assert!(self.queue.is_empty());

    }

    pub(crate) fn to_csv(&mut self, dir: &str) {
        let mut feature_file = File::create(format!("{}/listing_features.csv", dir)).expect("file creation failed");
        let mut history_file = File::create(format!("{}/listing_history.csv", dir)).unwrap();

        
        if let Err(e) = CsvWriter::new(&mut feature_file)
            .finish(&mut self.listing_features) {
                warn!("Error writing to csv: {}", e);
            }
        
        if let Err(e) = CsvWriter::new(&mut history_file)
            .finish(&mut self.listing_history) {
                warn!("Error writing to csv: {}", e);
            }

        
        
    }

    pub(crate) fn print_data_head(&self) {
        println!("{:?}", self.listing_history.head(Some(5)));

        println!("{:?}", self.listing_features.head(Some(5)));
    }
}

