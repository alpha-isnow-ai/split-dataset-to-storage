# Dataset Splitter and Storage

A tool for downloading Hugging Face datasets (specifically from [Papers with Backtest](https://paperswithbacktest.com), splitting them by month, and storing them in Cloudflare R2 storage.
Highly recommended to subscribe to the [Papers with Backtest](https://paperswithbacktest.com) service and you will get access to the same datasets on [Hugging Face](https://huggingface.co/paperswithbacktest). 

## Features

- Automatically downloads datasets from Hugging Face
- Splits data by month and converts to Parquet format
- Stores the processed data in Cloudflare R2
- Forces updates for the most recent six months of data
- Uses rclone for efficient incremental syncing to R2

## Setup

### Prerequisites

- Python 3.12
- Cloudflare R2 account and credentials
- Hugging Face account and credentials
- rclone (installed automatically in GitHub Actions)
- Git

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install pandas pyarrow datasets huggingface_hub boto3 s3fs python-dotenv
   sudo apt-get install rclone  # On Ubuntu/Debian
   # or
   brew install rclone  # On macOS
   ```

4. Create a `.env` file with your R2 credentials:
   ```
   R2_ENDPOINT_URL=https://xxxx.r2.cloudflarestorage.com
   R2_ACCESS_KEY_ID=your_access_key
   R2_SECRET_ACCESS_KEY=your_secret_key
   R2_BUCKET_NAME=your_bucket_name
   HF_TOKEN=your_huggingface_token
   ```

## Usage

### Manual Execution

Run the script to process all configured datasets:

```bash
python hf_to_R2.py
```

### Command-line Options

- `--overwrite-cache`: Process all months instead of just the recent six months
- `--force-sync`: Force sync all files to R2 (using rclone sync instead of copy)

## GitHub Actions Workflow

This repository includes a GitHub Actions workflow that automatically runs the data processing script:

- **Scheduled Execution**: Runs daily at midnight UTC
- **Manual Trigger**: Can be manually triggered from the GitHub Actions tab
- **Force Update**: Automatically runs when a commit with "[force update]" in the message is pushed

### Configuring GitHub Actions

To use the GitHub Actions workflow:

1. Add the following secrets to your GitHub repository:
   - `R2_ENDPOINT_URL`
   - `R2_ACCESS_KEY_ID`
   - `R2_SECRET_ACCESS_KEY`
   - `R2_BUCKET_NAME`
   - `HF_TOKEN`

2. Ensure the workflow file (`.github/workflows/dataset_update.yml`) is committed to your repository

3. To force an update, include "[force update]" in your commit message:
   ```bash
   git commit -m "Updated dataset configuration [force update]"
   ```

## How It Works

1. The script loads complete datasets from Hugging Face
2. Data is converted to Pandas DataFrames
3. The DataFrame is split by month
4. Each month is saved as a separate Parquet file in local cache
   - By default, only the most recent six calendar months are processed
   - With `--overwrite-cache`, all months are processed
5. rclone is used to sync files to R2 storage:
   - By default, only new or larger files are copied to R2
   - With `--force-sync`, all files are synchronized to R2
   - Files that exist in R2 but not locally are preserved

## Sync Strategy

The sync strategy uses rclone with the following behavior:
- Default mode (`rclone copy`): Only upload files that don't exist in R2 or are larger locally
- Force mode (`rclone sync` with `--force-sync` flag): Make R2 identical to local cache

In both cases, files that exist in R2 but not locally are preserved.

---

## Repositories

- [Stocks-Daily-Price](https://huggingface.co/paperswithbacktest/Stocks-Daily-Price)
- [ETFs-Daily-Price](https://huggingface.co/paperswithbacktest/ETFs-Daily-Price)
- [Indices-Daily-Price](https://huggingface.co/paperswithbacktest/Indices-Daily-Price)
- [Cryptocurrencies-Daily-Price](https://huggingface.co/paperswithbacktest/Cryptocurrencies-Daily-Price)
- [Bonds-Daily-Price](https://huggingface.co/paperswithbacktest/Bonds-Daily-Price)
- [Forex-Daily-Price](https://huggingface.co/paperswithbacktest/Forex-Daily-Price)
- [Commodities-Daily-Price](https://huggingface.co/paperswithbacktest/Commodities-Daily-Price)




