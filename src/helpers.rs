use scraper::{Html, Selector};
use anyhow::Result;
use tracing::{info, trace, warn};
use polars::prelude::*;
use crate::listing_structs::{ListingsContainer, PriceHistory};

pub(crate) async fn request(target_url: &str) -> Result<Html> {

    // Required ScraperAPI request params
    let params = [
        ("url", target_url),
        ("api_key", "0861bae719981ddf7ae64ddfcb5193ad")
    ];

    let scraper_url = "https://api.scraperapi.com/";
    let scraper_url = reqwest::Url::parse_with_params(scraper_url, params).unwrap();

    // Send Request
    let response = reqwest::get(scraper_url).await?;
    info!("Response Code: {}", response.status());

    // Convert resp to HTML str
    let response_str = response.text().await?;

    let parsed = Html::parse_document(&response_str);

    Ok(parsed)
}

impl ListingsContainer {
    pub(crate) fn initialize_dataset(&mut self) {
            
        // If local data exists, pull it in
        
        if !self.force_refresh {
            if let Ok(local_data) = CsvReader::from_path("out/listing_dataset.csv"){
                if let Ok(parsed) = local_data.has_header(true).finish() {
                    
                    self.data = parsed;
                    // TODO: Price History col
                    // let last = self.data["price_history"].get(0).unwrap(); 
                    // info!("HERE: {:?}", last);
                    
                    // // Check for expected columns
                    // let expected_cols: Vec<&str> = vec![];
                    // assert_eq!(expected_cols, self.data.get_column_names());

                    info!("Local data initialized, shape: {:?}", self.data.shape())
                    
                }
            }
        }
        // Else assign empty col dataframe
        else {
            info!("Local data ignored/not found.");

            let price_col = Series::new_empty("price", &DataType::UInt32);
            let beds_col = Series::new_empty("beds", &DataType::Int32);
            let baths_col = Series::new_empty("baths", &DataType::Int32);
            let sqft_col = Series::new_empty("sqft", &DataType::UInt32);
            let lot_size_col = Series::new_empty("lot_size", &DataType::Int32);
            let street_col = Series::new_empty("street", &DataType::Utf8);
            let apt_col = Series::new_empty("apt", &DataType::Int32);
            let city_col = Series::new_empty("city", &DataType::Utf8);
            let state_col = Series::new_empty("state", &DataType::Utf8);
            let zip_col = Series::new_empty("zip", &DataType::UInt32);
            let addr_str_col = Series::new_empty("addr_str", &DataType::Utf8);
            // let price_history_col = Series::new_empty("price_history", &DataType::Struct());
            let price_history_col = Series::new_empty("historical_prices", &DataType::List(Box::new(DataType::UInt16)));
            let date_history_col = Series::new_empty("historical_dates", &DataType::List(Box::new(DataType::Date)));

            let cols = vec![price_col, beds_col, baths_col, sqft_col, lot_size_col, street_col, 
            apt_col, city_col, state_col, zip_col, addr_str_col, price_history_col, date_history_col];

            self.data = DataFrame::new(cols).expect("Failed to create empty default");
        }

    }
}

// Extraction Helpers
