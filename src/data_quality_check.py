import polars as pl
import json
from pathlib import Path
import os
import ydata_profiling as yp

#Own Python files
from resale_flat_schema import raw_resale_flat_schema, cleaned_resale_flat_schema
from logging_function import logger

# Load config
config_path = Path(__file__).parent / "config.json"
rules_path = Path(__file__).parent / "data_quality_rules.json"
with open(config_path, "r") as f:
    config = json.load(f)


# Define config variables
folder_paths = config.get("FolderPaths", {})
raw_folder = config["FolderPaths"]["RawFolderPath"]
profile_report_path = config["ProfilingReport"]["ProfileReportPath"]
required_columns = config["ColumnNames"]



## Get files from raw data folder and parse into profiler 
def data_quality_run(reprofile = False):
    
    """
    Input: 
    Output: 

    1. Input all files into a given dataset 
    2. Send the dataset for profiling
    3. Export profile report into config ProfilingReport.ProfileReportPath value
    
    """
    # Do some data quality transformation, slice to Failed and ok then read into pl.read_csv with schema
    df = pl.read_csv(os.path.join(raw_folder, "*.csv" ), schema=raw_resale_flat_schema)
    logger.info(f"Loaded raw CSV files from {os.path.abspath(raw_folder)}")
    df_pandas = df.to_pandas()
    if reprofile or not os.path.exists(profile_report_path):
        profile = yp.ProfileReport(df_pandas, title=" Profiling Report for Resale HDB",  explorative=True)
        profile.to_file(profile_report_path)
        logger.info(f"Exported profiling report to {profile_report_path}")
    return df # return polars dataframe

def combine_datasets(data_folder: Path) -> pl.DataFrame:
    df = pl.read_csv(os.path.join(data_folder, "*.csv" ), schema=raw_resale_flat_schema)
    return df

def load_quality_rules() -> dict:
    """Read categorical rules from the JSON file and return the mapping.

    Returns:
        A dictionary keyed by column name, each containing a sub-dictionary
        with an ``expected_values`` list.
    """

    try:
        with open(rules_path, "r") as f:
            rules = json.load(f)
        logger.info(f"Loaded quality rules from {rules_path}")
        return rules.get("categorical_columns", {})
    except FileNotFoundError:
        logger.warning(f"Quality rules file not found at {rules_path}")
        return {}


def data_validation(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate dataframe using configuration rules.

    The function performs the following operations:

    2. For every column defined in ``data_quality_rules.json`` under
       ``categorical_columns``:
       * Converts values to uppercase.
       * Keeps only rows whose value is in the expected list.
    3. Splits the input into ``qualified`` and ``not_qualified`` frames.
    4. Casts the qualified frame back to ``cleaned_resale_flat_schema`` types,
       converting ``resale_price`` from formatted string to integer.

    Args:
        df: Input DataFrame.

    Returns:
        A tuple ``(qualified_df, not_qualified_df)``. Rows with null ``id``
        are removed entirely (they do not appear in either output).
    """
    categorical_rules = load_quality_rules()

    # 1. cast qualified dataframe to cleaned schema types
    non_numeric_df, qualified_df = filter_null_values(df)

    # 2. uppercase all categorical columns to standardize before filtering
    qualified_df = qualified_df.with_columns(pl.col(list(categorical_rules.keys())).str.to_uppercase())
    logger.info(
        f"Successfully converted all categorical values to upper case"
    )
    # Keep only values that are in the expected list for each column
    qualified_df, missing_from_expected_cat_df = filter_categorical_expected_values(categorical_rules, qualified_df)

    logger.info(
        f"Successfully filtered out categorical values based on rules: {qualified_df.height} qualified, {missing_from_expected_cat_df.height} not qualified"
    )

    qualified_df, df_duplicates_subset = filter_duplicate_rows(qualified_df)
    unqualified_df = pl.concat([non_numeric_df, missing_from_expected_cat_df, df_duplicates_subset])
    logger.info(
        f"Validation complete: {qualified_df.height} qualified, {unqualified_df.height} not qualified"
    )

    
    # Export to Cleaned and Failed files
    qualified_df.write_csv(config["FolderPaths"]["CleanedFolderName"])
    unqualified_df.write_csv(config["FolderPaths"]["FailedFolderName"])

    return qualified_df, unqualified_df


# Helper functions for filtering data quality issues
def filter_duplicate_rows(qualified_df):
    list_required_columns = qualified_df.columns
    list_required_columns.remove("resale_price")
    #3. Remove duplicate rows 
    qualified_df = qualified_df.with_columns(composite_key=pl.concat_str(list_required_columns, separator="-"))
    qualified_df.sort("resale_price", descending=True) # keep the one with highest resale price if there are duplicates
    qualified_unique_df = qualified_df.unique(subset=["composite_key"], keep="first")
    df_duplicates_subset = qualified_df.filter(pl.col("composite_key").is_duplicated()).drop("composite_key")
    return qualified_unique_df,df_duplicates_subset

def filter_null_values(df):
    df_cast = df.with_columns(pl.col("resale_price").str.to_integer(strict=False),
    pl.col("month").str.to_date(format="%Y-%m", strict=False),
    pl.col("floor_area_sqm").str.to_integer(strict=False))



    non_numeric_df = df_cast.filter(
        pl.col("resale_price").is_null() |
        pl.col("month").is_null() |
        pl.col("floor_area_sqm").is_null()
    )
    qualified_df = df_cast.filter(
    pl.col("resale_price").is_not_null() &
    pl.col("month").is_not_null() &
    pl.col("floor_area_sqm").is_not_null()
    )

    logger.info(
        f"Successfully converted and filtered out null values: {non_numeric_df.height} rows with null values found"
    )
    
    return non_numeric_df,qualified_df

def filter_categorical_expected_values(categorical_rules, qualified_df):
    for column, rule in categorical_rules.items():
        expected = rule.get("expected_values", [])
        if column in qualified_df.columns and expected:
            qualified_final_df = qualified_df.filter(pl.col(column).is_in(expected))
            missing_from_expected_cat_df = qualified_df.filter(~pl.col(column).is_in(expected))
    return qualified_final_df,missing_from_expected_cat_df


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
    combined_df = combine_datasets(raw_folder)
    # perform validation and capture rows that don't meet categorical rules
    qualified, not_qualified = data_validation(combined_df)
# # Step 4: Perform ETL transformations (placeholder for your logic)
# # Example: filter out rows with null 'id'
# transformed = combined.filter(pl.col("id").is_not_null())

# # Step 5: Export transformed dataset
# transformed_folder.mkdir(parents=True, exist_ok=True)
# transformed.write_csv(transformed_folder / "combined_transformed.csv")