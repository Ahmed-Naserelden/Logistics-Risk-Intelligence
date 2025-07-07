import requests
import datetime
import os
import logging
from urllib.parse import urljoin

#Environment variables for configuration.These can be set in the environment or default values will be used

Base_url = os.getenv("Base_url", "https://msi.nga.mil") 
Download_endpoint = os.getenv("download_endpoint", "/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv")
Data_dir = os.getenv("data_dir", "/extraction/data/ports")
File_prefix = os.getenv("file_prefix", "WPI") 
Log_file = os.getenv("Log_file", "/extraction/logs/download_ports_api.log")

def download_file_from_url():

    """Download a file from a specified URL and save it to a local directory."""
    
    os.makedirs(Data_dir, exist_ok=True) # Ensure the data directory exists, creating it if necessary.

    now = datetime.datetime.now() # Get the current date and time to create a unique file name.

    file_name = f"{File_prefix}_{now.strftime('%Y-%m')}.csv" # The file name includes a prefix and the current year and month.
    
    file_path = os.path.join(Data_dir, file_name) #Construct the full file path where the downloaded file will be saved.

    download_url = urljoin(Base_url, Download_endpoint) # Construct the full download URL by joining the base URL and the endpoint.
    try:
        response = requests.get(download_url)  # Send a GET request to the download URL.
        response.raise_for_status() 
        with open(file_path, 'wb') as file:  # Open the file in binary write mode.
            file.write(response.content)  # Write the content of the response to the file.
        logging.info(f"File downloaded successfully: {file_path}") 
    except requests.exceptions.RequestException as e: 
        logging.error(f"Error downloading file: {e}") 
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def logging_setup():

    """Set up logging configuration."""

    logging.basicConfig(
        filename=Log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def main():

    """Main function to set up logging and initiate the download process."""

    logging_setup()
    logging.info("Starting download process.")
    download_file_from_url()
    logging.info("Download process completed.")

if __name__ == "__main__":
    main()
