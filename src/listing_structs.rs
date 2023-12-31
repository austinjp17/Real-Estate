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
    pub(crate) data: DataFrame,
    pub(crate) last_update: Option<DateTime<Local>>,
    pub(crate) force_refresh: bool,
}

impl Default for ListingsContainer {
    fn default() -> Self {
        // TODO: Schema instead ??
        
        Self { 
            queue: vec![], 
            data: DataFrame::empty(),
            last_update: None,
            force_refresh: false,
        }
    }
}

impl ListingsContainer {
    /// Empty, no columns, dataframe
    pub(crate) fn new(force_refresh: bool) -> Self {
        ListingsContainer { queue: vec![], data: DataFrame::empty(), last_update: None, force_refresh }
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

        // Historical Components
        let price_history_type = DataType::List(Box::new(DataType::UInt32));
        let mut historical_prices = Series::new_empty("historical_prices", &price_history_type);

        let historical_dates_type = DataType::List(Box::new(DataType::Date));
        let mut historical_dates = Series::new_empty("historical_dates", &historical_dates_type);

        
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

            if let Err(e) = historical_prices.append(&Series::new("", vec![Box::new(listing.current_price)])) {
                panic!("Failed to initialize historical price container. Error: {}", e);
            }

            let unix_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
            if let Err(e) = historical_dates.append(&Series::new("", vec![unix_time])) {
                panic!("Failed to initialize historical dates container. Error: {}", e);
            }
            // historical_dates.push(vec![Local::now()]);
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


        let cols = vec![prices, beds, baths, sqft, lot_size, street, apt, city, state, zip, addr_str, historical_prices, historical_dates];
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

