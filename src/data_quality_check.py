import polars as pl
import json
from pathlib import Path
import os

#Own Python files
from resale_flat_schema import resale_flat_schema
from logging_function import logger

# Load config
config_path = Path(__file__).parent / "config.json"
with open(config_path, "r") as f:
    config = json.load(f)


folder_paths = config.get("FolderPaths", {})
raw_folder = config["FolderPaths"]["RawFolderPath"]

required_columns = config["ColumnNames"]


# Create missing folders
for name, path in folder_paths.items():
    folder = os.path.join(os.getcwd(), path)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f"Created folder: {folder}")
    else:
        logger.info(f"Folder already exists: {folder}")


## Get files from raw data folder
def get_raw_files():

# 
# Do some data quality transformation, slice to Failed and ok then read into pl.read_csv with schema
    df = pl.read_csv(os.path.join(raw_folder, "*.csv"), schema=resale_flat_schema)
    logger.info(f"Loaded raw CSV files from {os.path.abspath(raw_folder)}")
    # Step 2 (continued): Join/concatenate datasets
    combined = pl.concat(df)
    return combined # return polars dataframe

def export_raw_data(df: pl.DataFrame):
    raw_folder = Path(folder_paths.get("RawData", "raw_data"))
    # Step 3: Export combined raw dataset
    raw_folder.mkdir(parents=True, exist_ok=True)
    df.write_csv(raw_folder / "combined_raw.csv")
    logger.info(f"Exported combined raw dataset to {raw_folder / 'combined_raw.csv'}")


def get_column_resale_identifier(df: pl.DataFrame) -> pl.Series:
    """Generate a unique identifier for each resale record based on key columns."""
    # Create a composite key by concatenating relevant columns
    key_cols = required_columns
    # Ensure all key columns exist in the dataframe
    for col in key_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df_new_column = df.with_columns(pl.lit("S").alias("identifier"))

    df_new_column = df_new_column.with_columns(
        df_new_column["block"]
        .str.strip_chars()   # remove whitespace or other chars
        .str.zfill(3)        # pad with zeros until length = 3
        .alias("block")
    )

    # Extract year and month
    df_new_column = df_new_column.with_columns([
        df_new_column["date"].dt.year().alias("year"),
        df_new_column["date"].dt.month().alias("month")
    ])

    # Group by year, month, X, X3 and compute average of Resale
    agg_df = (
        df_new_column.group_by(["year", "month", "X", "X3"])
        .agg([
            pl.col("Resale").mean().alias("Resale_avg")
        ])
    )   

    # Join back to original DataFrame
    df = df.join(agg_df, on=["year", "month", "X", "X3"], how="left")
    df = df.with_columns([
        pl.concat_str([
            df["num_col"].cast(pl.Utf8),   # ensure numeric is string
            df["str_col"],
            df["extra_col"]
        ], separator="-").alias("combined")
    ])

    return df




if __name__ == "__main__":
    combined_df = get_raw_files()
    export_raw_data(combined_df)
    get_column_resale_identifier(combined_df)
# # Step 4: Perform ETL transformations (placeholder for your logic)
# # Example: filter out rows with null 'id'
# transformed = combined.filter(pl.col("id").is_not_null())

# # Step 5: Export transformed dataset
# transformed_folder.mkdir(parents=True, exist_ok=True)
# transformed.write_csv(transformed_folder / "combined_transformed.csv")