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

load_dotenv()

disable_progress_bar()

R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME = (
    os.getenv("R2_ENDPOINT_URL"),
    os.getenv("R2_ACCESS_KEY_ID"),
    os.getenv("R2_SECRET_ACCESS_KEY"),
    os.getenv("R2_BUCKET_NAME"),
)
if (
    not R2_ENDPOINT_URL
    or not R2_ACCESS_KEY_ID
    or not R2_SECRET_ACCESS_KEY
    or not R2_BUCKET_NAME
):
    raise ValueError(
        "R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME must be set"
    )


def get_dataset_last_update(repo_id):
    try:
        api = hf_api.HfApi()
        repo_info = api.repo_info(repo_id=repo_id, repo_type="dataset")
        return repo_info.lastModified

    except Exception as e:
        print(f"Error getting repository info: {e}")
        return None


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
        endpoint_url=R2_ENDPOINT_URL,
        key=R2_ACCESS_KEY_ID,
        secret=R2_SECRET_ACCESS_KEY,
        config_kwargs={"signature_version": "s3v4"},
    )


def check_file_exists_in_r2(r2_client, bucket_name, file_key):
    try:
        r2_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except:
        return False


def read_changelog_from_r2(r2_client, bucket_name, repo_name):
    changelog_key = f"ds/{repo_name}/changelog"
    try:
        response = r2_client.get_object(Bucket=bucket_name, Key=changelog_key)
        changelog_data = json.loads(response["Body"].read().decode("utf-8"))
        return changelog_data
    except:
        return None


def write_changelog_to_r2(r2_client, bucket_name, repo_name, last_update):
    """Write the changelog file to R2"""
    changelog_key = f"ds/{repo_name}/changelog"

    changelog_data = {
        "repo_id": repo_name,
        "last_update": last_update.isoformat(),
        "processed_at": datetime.now().isoformat(),
    }

    r2_client.put_object(
        Bucket=bucket_name,
        Key=changelog_key,
        Body=json.dumps(changelog_data, indent=2),
        ContentType="application/json",
    )

    print(f"Updated changelog in R2: {changelog_key}")


def process_dataset_by_month(repo_id, bucket_name, compression="zstd"):
    repo_name = repo_id.split("/")[-1].lower()
    r2_client = get_r2_client()
    last_update = get_dataset_last_update(repo_id)
    if not last_update:
        print(f"Could not get last update time for {repo_id}. Exiting.")
        return

    print(f"Repository {repo_id} last updated: {last_update.isoformat()}")

    changelog = read_changelog_from_r2(r2_client, bucket_name, repo_name)
    if changelog:
        last_processed = datetime.fromisoformat(changelog.get("last_update", None))
        if last_processed == None or last_update == None:
            pass
        if last_processed >= last_update:
            print(
                f"Repository has not been updated since last processing ({last_processed.isoformat()}). Exiting."
            )
            exit()

    print(f"Loading dataset {repo_id}...")
    start_time = time.time()
    dataset = load_dataset(repo_id)
    print(f"Dataset loaded in {time.time() - start_time:.2f} seconds")

    print("Converting to pandas DataFrame...")
    start_time = time.time()
    df = dataset["train"].to_pandas()
    print(f"Conversion completed in {time.time() - start_time:.2f} seconds")

    df["date"] = pd.to_datetime(df["date"])

    df["year_month"] = df["date"].dt.strftime("%Y.%m")

    months = sorted(df["year_month"].unique(), reverse=True)
    print(f"Found {len(months)} unique months in the dataset")

    fs = get_r2_filesystem()

    for month in months:
        file_key = f"ds/{repo_name}/{month}.parquet"

        if check_file_exists_in_r2(r2_client, bucket_name, file_key):
            print(f"Month {month} already exists in R2. Skipping processing.")
            continue

        print(f"Processing month {month}...")
        month_df = df[df["year_month"] == month].drop(columns=["year_month"])

        s3_path = f"s3://{bucket_name}/{file_key}"
        print(f"Saving {month} to R2 with {compression} compression...")
        start_time = time.time()

        month_df.to_parquet(
            s3_path, compression=compression, index=False, filesystem=fs
        )

        print(
            f"Saved {len(month_df)} records for {month} in {time.time() - start_time:.2f} seconds"
        )

    # Update the changelog with the latest processing information
    write_changelog_to_r2(r2_client, bucket_name, repo_name, last_update)
    print("Processing completed successfully!")


def load_month_from_r2(repo_name, month, bucket_name):
    fs = get_r2_filesystem()

    s3_path = f"s3://{bucket_name}/ds/{repo_name}/{month}.parquet"

    print(f"Loading {month} data from R2...")
    df = pd.read_parquet(s3_path, filesystem=fs)
    print(f"Loaded {len(df)} records")

    return df


def main():

    for repo_id in [
        "paperswithbacktest/Stocks-Daily-Price",
        "paperswithbacktest/ETFs-Daily-Price",
    ]:
        process_dataset_by_month(repo_id, R2_BUCKET_NAME, compression="zstd")


if __name__ == "__main__":
    main()
