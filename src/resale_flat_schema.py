import polars as pl

# Define schema explicitly
resale_flat_schema = {
    "month": pl.Utf8,               
    "town": pl.Utf8,                
    "flat_type": pl.Utf8,            
    "block": pl.Utf8,                
    "street_name": pl.Utf8,          
    "storey_range": pl.Utf8,         
    "floor_area_sqm": pl.Float64,    
    "flat_model": pl.Utf8,           
    "lease_commence_date": pl.Int32, 
    "remaining_lease": pl.Utf8,      # text like "89 years"
    "resale_price": pl.Decimal(precision=5, scale=2)      
}
