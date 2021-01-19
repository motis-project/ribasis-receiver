#!/usr/bin/env python3
import os
import ssl
import json
import logging
from pathlib import Path
from datetime import datetime

import dotenv
import pika
import requests


dotenv.load_dotenv()

CA_FILE = os.getenv("CA_FILE")
CLIENT_CERT_FILE = os.getenv("CLIENT_CERT_FILE")
HOST = os.getenv("HOST")
VIRTUAL_HOST = os.getenv("VIRTUAL_HOST")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", "5672"))
QUEUE = os.getenv("QUEUE")
DATA_DIR = Path(os.getenv("DATA_DIR"))
MOTIS_NOTIFY_URL = os.getenv("MOTIS_NOTIFY_URL")


def notify_motis():
    if not MOTIS_NOTIFY_URL:
        return
    try:
        requests.get(MOTIS_NOTIFY_URL, timeout=10)
    except Exception as e:
        print("MOTIS call failed: {}".format(e))


def on_message(channel, method_frame, header_frame, body):
    now = datetime.now()
    data_path = DATA_DIR / now.strftime("%Y%m%d")
    if not data_path.exists():
        data_path.mkdir(parents=True)

    j = json.loads(body.decode(header_frame.content_encoding or "utf-8"))
    msg_sequence = j["meta"]["sequence"]
    msg_id = j["meta"]["id"]
    file_name = "{}_{}_{}.json".format(
        int(now.timestamp() * 1000000), msg_sequence, msg_id
    )
    file_path = data_path / file_name
    with file_path.open("wb") as f:
        f.write(body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    print("{:13} {:36} {:32}".format(msg_sequence, msg_id, j["meta"]["created"]))
    notify_motis()

def main():
    ssl_options = None
    if CA_FILE:
        ssl_ctx = ssl.create_default_context(cafile=CA_FILE)
        ssl_ctx.load_cert_chain(CLIENT_CERT_FILE)
        ssl_options = pika.SSLOptions(ssl_ctx, HOST)
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    conn_params = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        virtual_host=VIRTUAL_HOST,
        credentials=credentials,
        ssl_options=ssl_options,
    )
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()
    channel.basic_consume(QUEUE, on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        try:
            connection.close()
        except:
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
