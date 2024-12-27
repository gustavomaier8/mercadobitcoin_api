# Mercado Bitcoin API to S3 Pipeline

This project fetches trade data from the Mercado Bitcoin API, processes the data into a CSV file, and uploads it to an Amazon S3 bucket.

## Features
- Fetch trading data for a specified symbol from the Mercado Bitcoin API.
- Convert the API response into a structured Pandas DataFrame.
- Save the DataFrame as a CSV file.
- Upload the CSV file to a specified folder in an S3 bucket.

## API Documentation
For detailed information about the Mercado Bitcoin API, check the official [API Documentation](https://api.mercadobitcoin.net/api/v4/docs?ref=public_apis&utm_medium=website#section/Overview).

## Requirements
- Python 3.7 or higher
- AWS credentials with access to S3
- Dependencies listed in `requirements.txt`

## Installation
1. Clone the repository:
   git clone https://github.com/yourusername/mercado-bitcoin-api-s3.git
   cd mercado-bitcoin-api-s3

2. Install dependencies:
    pip install -r requirements.txt

3. Create a .env file in the root directory with the following:
    AWS_ACCESS_KEY_ID=your_aws_access_key
    AWS_SECRET_ACCESS_KEY=your_aws_secret_key

## Usage
1. Edit the main() function in extraction.py to configure:
    - API URL
    - Symbol (e.g., BTC-BRL)
    - Local CSV path
    - S3 bucket name, folder, and region

2. Run the script:
    python script.py

## Dependencies
requests: For making API calls.
pandas: For processing data into a DataFrame.
boto3: For uploading files to S3.
python-dotenv: For managing environment variables.