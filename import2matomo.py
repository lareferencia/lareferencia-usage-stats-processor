import requests
import logging
import argparse
import gzip
import os

# Configurar el logger
def configure_logging(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler("matomo_events.log"),
                            logging.StreamHandler()
                        ])

def parse_arguments():
    parser = argparse.ArgumentParser(description='Send events to Matomo from a text file.')
    parser.add_argument('file_path', type=str, help='Path to the file containing events')
    parser.add_argument('matomo_url', type=str, help='Matomo server URL')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    return parser.parse_args()

def send_events_to_matomo(file_path, base_url):
    open_func = gzip.open if file_path.endswith('.gz') else open
    request_count = 0
    error_count = 0

    with open_func(file_path, 'rt') as file:
        for line in file:
            line = line.strip()
            if line:
                try:
                    response = requests.get(f"{base_url}{line}")
                    request_count += 1
                    logging.debug(f"Processing line: {line}")
                    if response.status_code == 200:
                        logging.debug(f"Event sent successfully: {line}")
                        error_count = 0  # Reset error count on success
                    else:
                        logging.error(f"Failed to send event: {line}, Status code: {response.status_code}")
                        error_count += 1
                except requests.RequestException as e:
                    logging.exception(f"Connection error while sending event: {line}, Error: {e}")
                    error_count += 1

                if request_count % 100 == 0:
                    print(f"Processed {request_count} requests so far.")

                if error_count >= 10:
                    logging.error("10 consecutive errors encountered. Stopping the process.")
                    break

    logging.info("Finished processing all events.")

if __name__ == "__main__":
    args = parse_arguments()
    configure_logging(args.debug)
    send_events_to_matomo(args.file_path, args.matomo_url)