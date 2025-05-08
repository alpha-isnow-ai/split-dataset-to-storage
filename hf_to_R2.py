import os
import time
import pandas as pd
import boto3
from botocore.client import Config
import s3fs
from datasets import load_dataset, disable_progress_bar
from huggingface_hub import hf_api
from datetime import datetime
import json
from dotenv import load_dotenv
import argparse
import pathlib
import subprocess

dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path, override=True)

disable_progress_bar()

# Set up argument parser
parser = argparse.ArgumentParser(
    description="Process HuggingFace datasets and upload to R2 storage"
)
parser.add_argument(
    "--overwrite-cache",
    action="store_true",
    help="Overwrite cached files even if they exist",
)
parser.add_argument(
    "--force-sync",
    action="store_true",
    help="Force sync all files to R2 even if they exist",
)
args = parser.parse_args()

# Cache directory
CACHE_DIR = os.path.expanduser("~/.alpha_isnow_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
HF_TOKEN = os.getenv("HF_TOKEN")


if (
    not R2_ENDPOINT_URL
    or not R2_ACCESS_KEY_ID
    or not R2_SECRET_ACCESS_KEY
    or not R2_BUCKET_NAME
    or not HF_TOKEN
):
    raise ValueError(
        "R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME and HF_TOKEN must be set"
    )


def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def get_r2_filesystem():
    return s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": R2_ENDPOINT_URL,
        },
        key=R2_ACCESS_KEY_ID,
        secret=R2_SECRET_ACCESS_KEY,
        config_kwargs={"signature_version": "s3v4"},
    )


def check_cache_file_exists(repo_name, month):
    cache_file = os.path.join(CACHE_DIR, f"{repo_name}_{month}.parquet")
    return os.path.exists(cache_file)


def get_cache_file_path(repo_name, month):
    return os.path.join(CACHE_DIR, f"{repo_name}_{month}.parquet")


def sync_files_to_r2(repo_name):
    # Create a temporary rclone config file
    rclone_config_path = os.path.join(CACHE_DIR, "rclone.conf")
    with open(rclone_config_path, "w") as f:
        f.write(
            f"""[r2]
type = s3
env_auth = true
provider = Cloudflare
access_key_id = {R2_ACCESS_KEY_ID}
secret_access_key = {R2_SECRET_ACCESS_KEY}
endpoint = {R2_ENDPOINT_URL}
"""
        )

    # Source directory with the repo's cached files
    source_dir = CACHE_DIR

    # Destination path in R2
    dest_path = f"r2:/{R2_BUCKET_NAME}/ds/{repo_name}/"

    # Build rclone command
    if args.force_sync:
        # When --force-sync is used, sync all files regardless of their state
        cmd = [
            "rclone",
            "sync",  # sync will make destination identical to source
            "--config",
            rclone_config_path,
            "--include",
            f"{repo_name}_*.parquet",
            "--s3-no-check-bucket",
        ]
        print(f"Force syncing all files to R2 for {repo_name}...")
    else:
        # Default behavior: only copy files that don't exist or are larger
        cmd = [
            "rclone",
            "copy",  # copy will only transfer missing/changed files
            "--config",
            rclone_config_path,
            "--include",
            f"{repo_name}_*.parquet",
            "--s3-no-check-bucket",
            "--size-only",  # Only consider size (ignore modification time)
            "--update",  # Skip files that are newer on the destination
        ]
        print(f"Syncing only new or larger files to R2 for {repo_name}...")

    # Add source and destination
    cmd.extend([source_dir, dest_path])

    # Execute the command
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"Sync completed successfully for {repo_name}")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error syncing files: {e}")
        print(f"Command output: {e.stdout}")
        print(f"Command error: {e.stderr}")
    # finally:
    #     # Clean up the temporary config file
    #     if os.path.exists(rclone_config_path):
    #         os.remove(rclone_config_path)


def process_dataset_by_month(repo_id, bucket_name, compression="brotli"):
    repo_name = repo_id.split("/")[-1].lower()

    print(f"Loading dataset {repo_id}...")
    start_time = time.time()
    dataset = load_dataset(repo_id, token=HF_TOKEN)
    print(f"Dataset loaded in {time.time() - start_time:.2f} seconds")

    print("Converting to pandas DataFrame...")
    start_time = time.time()
    df = dataset["train"].to_pandas()
    print(f"Conversion completed in {time.time() - start_time:.2f} seconds")

    df["date"] = pd.to_datetime(df["date"])
    df["year_month"] = df["date"].dt.strftime("%Y.%m")

    months = sorted(df["year_month"].unique(), reverse=True)
    print(f"Found {len(months)} unique months in the dataset")

    # Calculate the most recent six calendar months based on current date
    current_date = datetime.now()
    recent_months = []

    # Get the current month and previous five months (six months total)
    for i in range(6):
        # Calculate month offset from current date
        month_date = current_date.replace(day=1) - pd.DateOffset(months=i)
        recent_month = month_date.strftime("%Y.%m")
        recent_months.append(recent_month)

    print(f"Current date: {current_date.strftime('%Y-%m-%d')}")
    print(f"Will force update data from recent months: {recent_months}")

    for month in months:
        cache_file = get_cache_file_path(repo_name, month)

        # Process if it's a recent month or if overwrite-cache flag is set
        if args.overwrite_cache or month in recent_months:
            print(
                f"Processing month {month}"
                + (
                    ", overwrite-cache flag is set"
                    if args.overwrite_cache
                    else ", it's a recent month"
                )
            )

            month_df = df[df["year_month"] == month].drop(columns=["year_month"])

            # Save to cache
            print(f"Saving {month} to cache...")
            start_time = time.time()
            month_df.to_parquet(
                cache_file,
                engine="pyarrow",
                compression=compression,
                compression_level=11,
                index=False,
            )
            print(
                f"Saved {len(month_df)} records for {month} to cache in {time.time() - start_time:.2f} seconds"
            )
        else:
            print(
                f"Skipping month {month} because it's not in recent months and overwrite-cache is not set"
            )

    # Sync all files to R2 using rclone
    sync_files_to_r2(repo_name)
    print("Processing completed successfully!")


def main():
    for repo_id in [
        "paperswithbacktest/Stocks-Daily-Price",
        "paperswithbacktest/ETFs-Daily-Price",
        "paperswithbacktest/Indices-Daily-Price",
        "paperswithbacktest/Cryptocurrencies-Daily-Price",
        "paperswithbacktest/Bonds-Daily-Price",
        "paperswithbacktest/Forex-Daily-Price",
        "paperswithbacktest/Commodities-Daily-Price",
    ]:
        process_dataset_by_month(repo_id, R2_BUCKET_NAME, compression="zstd")


if __name__ == "__main__":
    main()
