import json
from pathlib import Path
import os 
import polars as pl
import hashlib

from resale_flat_schema import cleaned_resale_flat_schema, raw_resale_flat_schema, failed_resale_flat_schema
from logging_function import logger

# Load config
config_path = Path(__file__).parent / "config.json"
rules_path = Path(__file__).parent / "data_quality_rules.json"
with open(config_path, "r") as f:
    config = json.load(f)
with open(rules_path, "r") as f:
    rules = json.load(f)
    
def transform_cleaned_data():
    """Load the cleaned resale flat data, add intermediate block column, and
    compute average resale_price by month and flat_type.

    Returns
    -------
    pl.DataFrame
        Grouped DataFrame containing month, flat_type and average resale_price.
    """

    # load configuration and resolve cleaned file path
    logger.info("Loading configuration file")
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r", encoding="utf-8") as cfg:
        cfg_data = json.load(cfg)

    cleaned_path = Path(__file__).parent / cfg_data["FolderPaths"]["CleanedFolderName"]
    logger.info(f"Cleaned data path resolved: {cleaned_path}")

    # read using the schema defined in resale_flat_schema
    logger.info("Reading cleaned CSV into DataFrame")
    df = pl.read_csv(
        cleaned_path,
        has_header=True,
        dtypes=cleaned_resale_flat_schema,
        try_parse_dates=True,
    )
    logger.info(f"Loaded {df.height} rows")

    
    df_remove_duplicates, df_duplicates_subset = filter_resale_identifier(df)


    # Export to Cleaned and Failed files

    logger.info("Updating failed records CSV")
    update_csv(df_duplicates_subset, config["FolderPaths"]["FailedFolderName"])
    
    logger.info("Writing transformed data to CSV")
    df_remove_duplicates.write_csv(config["FolderPaths"]["TransformedFolderName"])

    df_hashed = get_hashed_dataset(df_remove_duplicates)
    logger.info("Writing hashed data to CSV")
    df_hashed.write_csv(config["FolderPaths"]["HashedFolderName"])


    return df_hashed

def filter_resale_identifier(df):
    df_with_resale_identifier = get_resale_identifier(df)
    logger.info("Sorting by resale_price and removing duplicates")
    df_with_resale_identifier = df_with_resale_identifier.sort("resale_price", descending=True, maintain_order=True)
    df_remove_duplicates = df_with_resale_identifier.unique(subset=["resale_identifier"], keep="first")

    # drop intermediate columns
    logger.info("Dropping intermediate columns")
    df_remove_duplicates = df_remove_duplicates.drop("block_num").drop("avg_resale_price") 

    logger.info("Extracting duplicate rows")
    df_duplicates_subset = df_with_resale_identifier.filter(
        pl.col("resale_identifier").is_duplicated()
    ).drop("resale_identifier")

    df_duplicates_subset = df_duplicates_subset.with_columns(
        pl.lit("filter_resale_identifier").alias("reason_for_fail"))
        
    return df_remove_duplicates,df_duplicates_subset

def get_hashed_dataset(df):
    def hash_row(row):
    # Convert row to string, encode, and hash
        return hashlib.sha256(str(row).encode('utf-8')).hexdigest()

    # Apply the hashed column
    hashed_df = df.with_columns(
        pl.struct(["resale_identifier"]).map_elements(hash_row).alias("hashed_resale_identifier")
    )
    return hashed_df

def get_resale_identifier(df):

    # create intermediate column 'block_num' by stripping non-digits and zero-padding
    logger.info("Creating block_num column")
    df = df.with_columns(
        pl.col("block")
        .str.replace_all(r"[^0-9]", "")
        .str.zfill(3)
        .alias("block_num")
    )

    # compute average resale price grouped by month and flat_type
    logger.info("Aggregating average resale_price by month and flat_type")
    grouped = (
        df.group_by(["month", "flat_type"])
        .agg(pl.col("resale_price").mean().alias("avg_resale_price"))
        .sort(["month", "flat_type"])
    )
    logger.info(f"Aggregation produced {grouped.height} groups")

    # join the aggregated averages back to the original dataframe so that every
    # row receives its corresponding avg_resale_price value
    logger.info("Joining averages back to original DataFrame")
    df_with_avg = df.join(grouped, on=["month", "flat_type"], how="left")
    logger.info(f"Joined DataFrame has {df_with_avg.height} rows")



    logger.info("Generating resale_identifier column")
    df_with_resale_identifier = df_with_avg.with_columns([
    pl.format(
        "S{}{}{}{}",
        pl.col("block_num"),
        (pl.col("avg_resale_price").cast(pl.Int64) % 100),   # last 2 digits
        pl.col("month").dt.month(),
        pl.col("town").str.slice(0, 1)                      # drop first char
    ).alias("resale_identifier")
    ])
    
    return df_with_resale_identifier

def update_csv(df: pl.DataFrame, path: str) -> None:
    """
    Update or create a CSV at the given path:
    - If CSV does not exist, create it with df.
    - If CSV exists, filter df to same columns, append, and overwrite.
    - Return the updated DataFrame.
    """
    if not os.path.exists(path):
        # Create new CSV
        df.write_csv(path)
        return df
    else:
        # Read existing CSV
        existing = pl.read_csv(path,         
        has_header=True, 
        schema=failed_resale_flat_schema
    )
        
        # Align df to existing columns
        aligned = df.select([col for col in existing.columns if col in df.columns]).cast(failed_resale_flat_schema)


        # Append
        updated = pl.concat([existing, aligned], how="vertical")
        
        updated.write_csv(path)
    
if __name__ == "__main__":
    result = transform_cleaned_data()
