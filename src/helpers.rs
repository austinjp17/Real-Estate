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
    pub(crate) fn initialize_datasets(&mut self) {
            
        // If local data exists, pull it in
        // Initalize local feature data
        let feature_schema = Schema::from_iter(
            vec![
                Field::new("beds", DataType::Int32),
                Field::new("baths", DataType::Int32),
                Field::new("sqft", DataType::UInt32),
                Field::new("lot_size", DataType::Int32),
                Field::new("street", DataType::Utf8),
                Field::new("apt", DataType::Int32),
                Field::new("city", DataType::Utf8),
                Field::new("state", DataType::Utf8),
                Field::new("zip", DataType::UInt32),
                Field::new("addr_str", DataType::Utf8),
            ].into_iter()
        );
        if !self.force_refresh {
            if let Ok(local_feature_data) = CsvReader::from_path("out/listing_features.csv"){
                if let Ok(features_df) = local_feature_data
                    .has_header(true)
                    .with_schema(Some(Arc::new(feature_schema)))
                    .finish() {
                    
                    self.listing_features = features_df;
                    // TODO: Price History col
                    // let last = self.data["price_history"].get(0).unwrap(); 
                    // info!("HERE: {:?}", last);
                    
                    // // Check for expected columns
                    // let expected_cols: Vec<&str> = vec![];
                    // assert_eq!(expected_cols, self.data.get_column_names());

                    info!("Local data initialized, shape: {:?}", self.listing_features.shape())
                    
                }
            }
        }
        // Else assign empty col dataframe
        else {
            info!("Local feature data ignored/not found.");

            
            let feature_cols: Vec<Series> = feature_schema
                .iter()
                .map(|(col_name, dtype)| Series::new_empty(&col_name, &dtype))
                .collect();
            
            self.listing_features = DataFrame::new(feature_cols).expect("Failed to create empty default");
        }

        // Initalize local historical data
        let hist_schema = Schema::from_iter(
            vec![
                Field::new("addr_str", DataType::Utf8), 
                Field::new("date", DataType::UInt32),
                Field::new("price", DataType::UInt32),
            ].into_iter()
        );
        if !self.force_refresh {
            if let Ok(local_hist_data) = CsvReader::from_path("out/listing_history.csv"){
                if let Ok(hist_df) = local_hist_data
                    .has_header(true)
                    .with_schema(Some(Arc::new(hist_schema)))
                    .finish() {
                    
                    self.listing_history = hist_df;
                    

                    info!("Local data initialized, shape: {:?}", self.listing_history.shape())
                    
                }
            }
        }

        else {
            info!("Local price history data ignored/not found.");
            
            
            let hist_cols: Vec<Series> = hist_schema.iter().map(|(col_name, dtype)| Series::new_empty(&col_name, &dtype)).collect();
            self.listing_history = DataFrame::new(hist_cols).expect("Failed to create empty listing history DF");
        }
        

    }
}

// Extraction Helpers
