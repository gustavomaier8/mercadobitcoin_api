import requests
import pandas as pd
import os
import boto3
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv



def get_api_trades(api_url : str, symbol : str) -> list:
    """
        Fetches trade data from the specified API for a given symbol.

        :param api_url: The base URL of the API.
        :param symbol: The trading pair symbol (e.g., "BTC-BRL").
        :return: A list of trade data returned by the API.
        :raises ValueError: If there is an issue with the API request.
    """
        
    try:
        response = requests.get(f"{api_url}/{symbol}/trades")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error during API request: {e}") from e


def generate_dataframe(trades : List[Dict]) -> pd.DataFrame:
    """
        Converts a list of trade data into a Pandas DataFrame.

        :param trades: A list of dictionaries containing trade data.
        :return: A Pandas DataFrame containing the trade data.
        :raises ValueError: If the input data is not a valid list of dictionaries.
    """
        
    if not isinstance(trades, list):
        raise ValueError("The 'trades' parameter must be a list.")

    if not all(isinstance(item, dict) for item in trades):
        raise ValueError("All elements of the 'trades' list must be dictionaries.")

    trades_df = pd.DataFrame(trades)
    return trades_df


def dataframe_to_csv(df : pd.DataFrame, csv_path : str):
    """
        Saves a Pandas DataFrame as a CSV file in the specified directory.

        :param df: The DataFrame to save.
        :param csv_path: The directory path where the CSV will be saved.
        :return: The full file path of the saved CSV file.
        :raises ValueError: If the input is not a DataFrame, the directory path is invalid, 
                         or an error occurs while saving the CSV file.
    """
        
    if not isinstance(df, pd.DataFrame):
        raise ValueError("The 'df' parameter must be a Pandas DataFrame.")
    
    if not os.path.isdir(csv_path):
        raise ValueError(f"The path '{csv_path}' is not a valid directory.")
    
    today = datetime.today().strftime("%Y-%m-%d")
    filename = f"api_trades_{today}.csv"
    file_path = os.path.join(csv_path, filename)

    try:
        df.to_csv(file_path, index=False)
        print(f"DataFrame successfully saved to {file_path}")
        return file_path
    except Exception as e:
        raise ValueError(f"Error while saving the data on '{file_path}': {e}")


def upload_csv_to_s3(file_path : str, bucket_name : str, s3_folder : str, aws_access_key_id : str, aws_secret_access_key : str, aws_region : str):
    """
        Uploads a CSV file to a specified folder in an S3 bucket.

        :param file_path: The local file path of the CSV to upload.
        :param bucket_name: The name of the S3 bucket.
        :param s3_folder: The folder path in the S3 bucket.
        :param aws_access_key_id: AWS access key ID for authentication.
        :param aws_secret_access_key: AWS secret access key for authentication.
        :param aws_region: The AWS region where the bucket is located.
        :raises ValueError: If an error occurs during the upload process.
    """
    
    s3_client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name = aws_region
    )

    csv_file_name = file_path.split('\\')[-1]
    object = f"{s3_folder}/{csv_file_name}"

    try:
        s3_client.upload_file(Filename = file_path, Bucket = bucket_name, Key = object)
        print(f"File '{file_path}' uploaded successfully to bucket {bucket_name}.")
    except Exception as e:
        raise ValueError(f"An error occurred while uploading the file: {e}")


def main():
    """
        Main function to fetch trades from an API, save the data as a CSV, 
        and upload it to an S3 bucket.
    """
        
    load_dotenv()

    API_URL = "https://api.mercadobitcoin.net/api/v4"
    SYMBOL = "BTC-BRL"
    CSV_PATH = "C:\\data\\github\\mercadobitcoin_api"
    BUCKET_NAME = "mercadobitcoin-api"
    S3_FOLDER = "trades"
    AWS_REGION = "us-east-1"
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    api_trades = get_api_trades(API_URL, SYMBOL)
    trades_df = generate_dataframe(api_trades)
    csv_file_path = dataframe_to_csv(trades_df, CSV_PATH)
    upload_csv_to_s3(csv_file_path, BUCKET_NAME, S3_FOLDER, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)


if __name__ == "__main__":
    main()