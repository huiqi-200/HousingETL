import polars as pl
import json
from pathlib import Path
import os
import ydata_profiling as yp
from datetime import date, datetime

# Own Python files
from resale_flat_schema import raw_resale_flat_schema, cleaned_resale_flat_schema
from logging_function import logger

# Load config
config_path = Path(__file__).parent / "config.json"
rules_path = Path(__file__).parent / "data_quality_rules.json"
with open(config_path, "r") as f:
    config = json.load(f)
with open(rules_path, "r") as f:
    rules = json.load(f)


# Define config variables
folder_paths = config.get("FolderPaths", {})
raw_folder = config["FolderPaths"]["RawFolderPath"]
profile_report_path = config["ProfilingReport"]["ProfileReportPath"]
required_columns = config["ColumnNames"]


## Get files from raw data folder and parse into profiler
def data_profiling_run(reprofile=False):
    """
     Input:
     Output:
    1. Input all files into a given dataset
     2. Send the dataset for profiling
     3. Export profile report into config ProfilingReport.ProfileReportPath value

    """
    # Do some data quality transformation, slice to Failed and ok then read into pl.read_csv with schema
    df = pl.read_csv(os.path.join(raw_folder, "*.csv"), schema=raw_resale_flat_schema)
    logger.info(f"Loaded raw CSV files from {os.path.abspath(raw_folder)}")
    df_pandas = df.to_pandas()
    if reprofile or not os.path.exists(profile_report_path):
        profile = yp.ProfileReport(
            df_pandas, title=" Profiling Report for Resale HDB", explorative=True
        )
        profile.to_file(profile_report_path)
        logger.info(f"Exported profiling report to {profile_report_path}")
    return df  # return polars dataframe


def combine_datasets(data_folder: Path) -> pl.DataFrame:
    df = pl.read_csv(os.path.join(data_folder, "*.csv"), schema=raw_resale_flat_schema)
    df = df.with_columns(pl.lit(datetime.now()).alias("created_datetime"))
    return df


def load_quality_rules() -> dict:
    return rules.get("categorical_columns", {})


def data_validation(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate dataframe using configuration rules.

    The function performs the following operations:
    1. Cast numeric and date types
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
    # 2. Filter for dates required in test – now returns both good/failed
    qualified_df= filter_month_range(qualified_df)

    # 2. uppercase all categorical columns to standardize before filtering
    qualified_df = qualified_df.with_columns(
        pl.col(list(categorical_rules.keys())).str.to_uppercase()
    )
    logger.info(f"Successfully converted all categorical values to upper case")
    # Keep only values that are in the expected list for each column
    qualified_df, missing_from_expected_cat_df = filter_categorical_expected_values(
        categorical_rules, qualified_df
    )

    logger.info(
        f"Successfully filtered out categorical values based on rules: {qualified_df.height} qualified, {missing_from_expected_cat_df.height} not qualified"
    )
    # calculate lease
    qualified_df = get_remaining_lease(qualified_df)

    # keep only unique values based on composite key rule
    qualified_df, df_duplicates_subset = filter_duplicate_rows(qualified_df)

    unqualified_df = pl.concat(
        [
            non_numeric_df,
            missing_from_expected_cat_df,
            df_duplicates_subset,
        ]
    )

    logger.info(
        f"Validation complete: {qualified_df.height} qualified, {unqualified_df.height} not qualified"
    )

    # Export to Cleaned and Failed files
    qualified_df.write_csv(config["FolderPaths"]["CleanedFolderName"])
    unqualified_df.write_csv(config["FolderPaths"]["FailedFolderName"])

    return qualified_df, unqualified_df


def get_remaining_lease(qualified_df):
    leased_df = qualified_df.with_columns(
        [
            (
                (
                    int(rules["numerical_columns"]["remaining_lease"]["max"])
                    - (
                        pl.col("month").dt.year()
                        - pl.col("lease_commence_date").dt.year()
                        - 1
                    )
                ).alias("remaining_years")
            ),
            ((12 - pl.col("month").dt.month()).alias("remaining_months")),
        ]
    )
    leased_df = leased_df.with_columns(
        [
            pl.format(
                "{} years {} months",
                pl.col("remaining_years"),
                pl.col("remaining_months"),
            ).alias("remaining_lease")
        ]
    )
    leased_df = leased_df.drop(["remaining_years", "remaining_months"])
    leased_df = leased_df.with_columns(pl.lit(datetime.now()).alias("created_datetime"))
    return leased_df


def filter_month_range(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return only rows where ``month`` lies in value specified …"""
    start = datetime.strptime(rules["filters"]["month"]["min_month"], "%Y-%m")
    end = datetime.strptime(rules["filters"]["month"]["max_month"], "%Y-%m")
    within = df.filter(pl.col("month").is_between(start, end))

    logger.info(f"Filtered rows by month range: kept {within.height} of {df.height}")
    return within


# Helper functions for filtering data quality issues
def filter_duplicate_rows(qualified_df):
    """
    Using composite key (all columns except resale_price), sort and keep only highest priced row
    Input:
      qualified_df: polars.Dataframe
    Output:
      tuple of (qualified_df, df_duplicates_subset)
      qualified_df: polars.Dataframe with duplicates removed, keeping the one with highest resale price
       df_duplicates_subset: polars.Dataframe with only the duplicate rows that were removed from qualified_df
    """
    list_required_columns = qualified_df.columns
    list_required_columns.remove("resale_price")
    # 3. Remove duplicate rows
    qualified_df = qualified_df.with_columns(
        composite_key=pl.concat_str(list_required_columns, separator="")
    )
    qualified_df = qualified_df.sort(
        "resale_price", descending=True, maintain_order=True 
    )  # keep the one with highest resale price if there are duplicates
    qualified_unique_df = qualified_df.unique(subset=["composite_key"], keep="first")
    df_duplicates_subset = qualified_df.filter(
        pl.col("composite_key").is_duplicated()
    ).drop("composite_key")
    df_duplicates_subset = df_duplicates_subset.with_columns(
        [
            pl.lit(datetime.now()).alias("created_datetime"),
            pl.lit("filter_duplicate_rows").alias("reason_for_fail"),
        ]
    )
    qualified_unique_df = qualified_unique_df.with_columns(
        pl.lit(datetime.now()).alias("created_datetime")
    )
    return qualified_unique_df, df_duplicates_subset


def filter_null_values(df):
    df_cast = df.with_columns(
        [
            pl.col("resale_price").str.to_integer(strict=False),
            pl.col("month").str.to_date(format="%Y-%m", strict=False),
            pl.col("lease_commence_date").str.to_date(format="%Y", strict=False),
            pl.col("floor_area_sqm").str.to_integer(strict=False),
        ]
    )

    non_numeric_df = df_cast.filter(
        pl.col("resale_price").is_null()
        | pl.col("month").is_null()
        | pl.col("lease_commence_date").is_null()
        | pl.col("floor_area_sqm").is_null()
    ).with_columns(
        [
            pl.lit(datetime.now()).alias("created_datetime"),
            pl.lit("filter_null_values").alias("reason_for_fail"),
        ]
    )
    qualified_df = df_cast.filter(
        pl.col("resale_price").is_not_null()
        & pl.col("month").is_not_null()
        & pl.col("lease_commence_date").is_not_null()
        & pl.col("floor_area_sqm").is_not_null()
    ).with_columns(pl.lit(datetime.now()).alias("created_datetime"))

    logger.info(
        f"Successfully converted and filtered out null values: {non_numeric_df.height} rows with null values found"
    )

    return non_numeric_df, qualified_df


def filter_categorical_expected_values(categorical_rules, qualified_df):
    qualified_final_df = qualified_df
    missing_from_expected_cat_df = pl.DataFrame()
    for column, rule in categorical_rules.items():
        expected = rule.get("expected_values", [])
        if column in qualified_df.columns and expected:
            qualified_final_df = qualified_final_df.filter(
                pl.col(column).is_in(expected)
            )
            missing_from_expected_cat_df = qualified_df.filter(
                ~pl.col(column).is_in(expected)
            )
    missing_from_expected_cat_df = missing_from_expected_cat_df.with_columns(
        [
            pl.lit(datetime.now()).alias("created_datetime"),
            pl.lit("filter_categorical_expected_values").alias("reason_for_fail"),
        ]
    )
    qualified_final_df = qualified_final_df.with_columns(
        pl.lit(datetime.now()).alias("created_datetime")
    )
    return qualified_final_df, missing_from_expected_cat_df


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
