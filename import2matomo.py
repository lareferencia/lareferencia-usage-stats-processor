import argparse
import gzip
import logging
import requests
import json

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("matomo_events.log"),
                        logging.StreamHandler()
                    ])

def parse_arguments():
    parser = argparse.ArgumentParser(description='Send events to Matomo from a text file.')
    parser.add_argument('file_path', type=str, help='Path to the file containing events')
    parser.add_argument('matomo_url', type=str, help='Matomo server URL')
    parser.add_argument('token_auth', type=str, help='Matomo authentication token')
    parser.add_argument('--batch_size', type=int, default=100, help='Number of events to send in each batch')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    return parser.parse_args()

def send_events_to_matomo(file_path, base_url, token_auth, batch_size):
    open_func = gzip.open if file_path.endswith('.gz') else open
    request_count = 0
    error_count = 0
    request_list = []

    with open_func(file_path, 'rt') as file:
        for line in file:
            line = line.strip()
            if line:
                request_list.append(line)
                request_count += 1

                if len(request_list) >= batch_size:  # Send requests in batches
                    try:
                        data = json.dumps({'requests': request_list, 'token_auth': token_auth})
                        response = requests.post(base_url, data=data, verify=False)
                        if response.status_code == 200:
                            print(f"Batch sent successfully")
                            logging.debug(f"Batch sent successfully: {request_list}")
                            error_count = 0  # Reset error count on success
                        else:
                            logging.error(f"Failed to send batch: {request_list}, Status code: {response.status_code}")
                            error_count += 1
                    except requests.RequestException as e:
                        logging.exception(f"Connection error while sending batch: {request_list}, Error: {e}")
                        error_count += 1

                    request_list = []  # Reset request list after sending

                if request_count % batch_size == 0:
                    print(f"Processed {request_count} requests so far.")

                if error_count >= 3:
                    logging.error("3 consecutive errors encountered. Stopping the process.")
                    break

        # Send any remaining requests
        if request_list:
            try:
                data = json.dumps({'requests': request_list, 'token_auth': token_auth})
                response = requests.post(base_url, data=data, verify=False)
                if response.status_code == 200:
                    logging.debug(f"Final batch sent successfully: {request_list}")
                else:
                    logging.error(f"Failed to send final batch: {request_list}, Status code: {response.status_code}")
            except requests.RequestException as e:
                logging.exception(f"Connection error while sending final batch: {request_list}, Error: {e}")

    logging.info("Finished processing all events.")

if __name__ == "__main__":
    
    args = parse_arguments()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    send_events_to_matomo(args.file_path, args.matomo_url, args.token_auth, args.batch_size)