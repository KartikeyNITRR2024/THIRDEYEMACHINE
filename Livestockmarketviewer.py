import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time 
from dateutil import parser
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

STOCKS_MANAGER_BASE_URL = os.getenv("STOCKS_MANAGER_BASE_URL", "https://google.com")
STOCK_MARKET_VIEWER_BASE_URL = os.getenv("STOCK_MARKET_VIEWER_BASE_URL", "https://google.com")
MORNING_PRICE_UPDATER_BASE_URL = os.getenv("MORNING_PRICE_UPDATER_BASE_URL", "https://google.com")

# Constants
UNIQUEID = 126
MACHINE_NO = int(os.getenv("MACHINE_NO", 1))

# Construct API URLs
STOCKBATCH_API = f"{STOCKS_MANAGER_BASE_URL}/api/stocksbatch/{UNIQUEID}/stocksbatch/{MACHINE_NO}"
LIVE_STOCK_API = "https://www.google.com/finance/quote/"
SEND_DATA_API = f"{STOCK_MARKET_VIEWER_BASE_URL}/api/livemarket/124/Machine{MACHINE_NO}"
MORNING_API = f"{MORNING_PRICE_UPDATER_BASE_URL}/api/morningstockprice/morninglivemarketdata/130/Machine{MACHINE_NO}/{MACHINE_NO}"

TIME_TO_SEND_PAYLOAD = None
TIME_TO_SEND_MORNINGDATA = None
live_stock_payloads = []

class LiveStockPayload:
    def __init__(self, batch_id, stock_id, time, price):
        self.batch_id = batch_id
        self.stock_id = stock_id
        self.time = time
        self.price = price

    def to_dict(self):
        return {
            "batchId": self.batch_id,
            "stockId": self.stock_id,
            "time": self.time.isoformat(),  # Convert to ISO format for JSON
            "price": self.price
        }

    def __repr__(self):
        return f"LiveStockPayload(batchId={self.batch_id}, stockId={self.stock_id}, time={self.time}, price={self.price})"

stocklist = []

def fetch_api_data(api_url):
    logging.info("Fetching live stock data to send.")
    global stocklist
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        stocklist = [[key, value] for key, value in response.json().items()]
        return True

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error occurred: {req_err}") 
    except ValueError:
        logging.error("Failed to decode JSON response")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return False

def fetch_live_stock_info(key, value):
    stock_info = value.split(" ")
    url = f"{LIVE_STOCK_API}{stock_info[0]}:{stock_info[1]}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            class1 = "YMlKec fxKbKc"
            price_element = soup.find(class_=class1)
            if price_element:
                price_text = price_element.text.strip()
                try:
                    price = float(price_text[1:].replace(",", ""))
                    payload = LiveStockPayload(batch_id=MACHINE_NO, stock_id=key, time=datetime.now(), price=price)
                    live_stock_payloads.append(payload)
                except ValueError:
                    logging.error(f"Could not parse price for {key}: {price_text}")
            else:
                logging.error(f"Price element not found for {key}")
        else:
            logging.error(f"Failed to fetch price for {key}. Status code: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"Error fetching price for {key}: {e}")

def create_time_to_send_payload():
    global TIME_TO_SEND_PAYLOAD
    global TIME_TO_SEND_MORNINGDATA
    current_time = datetime.now(timezone.utc)  # Make current time aware
    next_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
    TIME_TO_SEND_PAYLOAD = next_minute + timedelta(seconds=MACHINE_NO-1)
    TIME_TO_SEND_MORNINGDATA = TIME_TO_SEND_PAYLOAD
    logging.info(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")
    logging.info(f"Updated TIME_TO_SEND_MORNINGDATA: {TIME_TO_SEND_MORNINGDATA}")

def update_time_to_send_payload(next_time):
    global TIME_TO_SEND_PAYLOAD
    parsed_time = parser.isoparse(next_time)
    adjusted_time = parsed_time - timedelta(hours=5, minutes=30)
    TIME_TO_SEND_PAYLOAD = adjusted_time
    logging.info(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")

def update_time_to_send_morning_payload(next_time):
    global TIME_TO_SEND_MORNINGDATA
    parsed_time = parser.isoparse(next_time)
    adjusted_time = parsed_time - timedelta(hours=5, minutes=30)
    TIME_TO_SEND_MORNINGDATA = adjusted_time
    logging.info(f"Updated TIME_TO_SEND_MORNINGDATA: {TIME_TO_SEND_MORNINGDATA}")

def send_live_market_data():
    global TIME_TO_SEND_PAYLOAD
    global live_stock_payloads
    if not live_stock_payloads:
        logging.info("No live stock data to send.")
        return

    # Convert payloads to a list of dictionaries
    payload_data = [payload.to_dict() for payload in live_stock_payloads]

    # Update TIME_TO_SEND_PAYLOAD if it's None
    if TIME_TO_SEND_PAYLOAD is None:
        create_time_to_send_payload()

    while datetime.now(timezone.utc) < TIME_TO_SEND_PAYLOAD:
        remaining_time = (TIME_TO_SEND_PAYLOAD - datetime.now(timezone.utc)).total_seconds()
        if remaining_time > 0:
            logging.info(f"Remaining Time: {remaining_time}")
            time.sleep(remaining_time) 

    remaining_time_for_morning_data = (TIME_TO_SEND_MORNINGDATA - datetime.now(timezone.utc)).total_seconds()
    
    if remaining_time_for_morning_data <= 0:
        try:
            response = requests.post(MORNING_API, json=payload_data)
            response.raise_for_status()
            logging.info(f"Successfully sent live stock data to morning data microservices. Response: {response.json()}")
            next_time = response.json()
            if next_time:
                update_time_to_send_morning_payload(next_time)
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred while sending data: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred while sending data: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout error occurred while sending data: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Error occurred while sending data: {req_err}")

    # Send data to the API
    try:
        response = requests.post(SEND_DATA_API, json=payload_data)
        response.raise_for_status()
        logging.info(f"Successfully sent live stock data. Response: {response.json()}")
        check = response.json().get('updateData')
        next_time = response.json().get('nextIterationTime')
        live_stock_payloads = []
        if next_time:
            update_time_to_send_payload(next_time)
        if check:
            fetch_api_data(STOCKBATCH_API)
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while sending data: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred while sending data: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred while sending data: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error occurred while sending data: {req_err}")

if __name__ == "__main__":
    status = fetch_api_data(STOCKBATCH_API)
    
    if status:
        num_workers = len(stocklist)
        attempt = 0
        max_attempts = 30  # Run the live stock fetching process 5 times
        while attempt >= 0:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_stock = {executor.submit(fetch_live_stock_info, key, value): (key, value) for key, value in stocklist}
                for future in as_completed(future_to_stock):
                    key, value = future_to_stock[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error processing {key} - {value}: {e}")

            # Send the collected live stock data to the specified API, but only once when it's time
            send_live_market_data()

            attempt += 1  # Increment the attempt counter

    else:
        logging.error("No data fetched due to an error.")
