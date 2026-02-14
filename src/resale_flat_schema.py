import polars as pl

"""
 For raw dataset, due to the nature of the data, we will read all columns as string (Utf8) except for floor_area_sqm and lease_commence_date which are numeric.
To identify types of each category first, and add the 

"""
raw_resale_flat_schema = {
    "month": pl.Utf8,               
    "town": pl.Utf8,                
    "flat_type": pl.Utf8,            
    "block": pl.Utf8,                
    "street_name": pl.Utf8,          
    "storey_range": pl.Utf8,         
    "floor_area_sqm": pl.Utf8,    
    "flat_model": pl.Utf8,           
    "lease_commence_date": pl.Utf8, 
    "remaining_lease": pl.Utf8,      # text like "89 years"
    "resale_price": pl.Utf8      
}

cleaned_resale_flat_schema = {
    "year": pl.Int32,
    "month": pl.Date(),               
    "town": pl.Utf8,                
    "flat_type": pl.Utf8,            
    "block": pl.Utf8,                
    "street_name": pl.Utf8,          
    "storey_range": pl.Utf8,         
    "floor_area_sqm": pl.Int64,    
    "flat_model": pl.Utf8,           
    "lease_commence_date": pl.Utf8, 
    "remaining_lease": pl.Utf8,      # text like "89 years"
    "resale_price": pl.Int64   
}
