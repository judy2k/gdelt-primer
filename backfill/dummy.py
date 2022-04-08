from concurrent.futures import thread
from queue import Queue
import subprocess
import threading

import io
from itertools import *
from zipfile import ZipFile

import requests

from gmloader.master import master_exports


def zip_downloader(input: Queue, output: Queue):
    while True:
        url = input.get()
        try:
            print(f"Downloading {url}")
            zip_data = requests.get(url).data
            print(f"Downloaded  {url}")
            output.put(zip_data)
        finally:
            input.task_done()


def zcat(input: Queue, output: Queue):
    while True:
        data = input.get()
        try:
            with ZipFile(io.BytesIO(data)) as z:
                zip_items = z.infolist()
                for item in zip_items:
                    if item.filename.endswith("CSV"):
                        print(f"{item.filename}")
        finally:
            input.task_done()


url_queue = Queue(5)
zip_queue = Queue()

downloading_threads = [
    threading.Thread(target=zip_downloader, args=[url_queue, zip_queue])
    for _ in range(10)
]
unzipping_threads = [
    threading.Thread(target=zcat, args=[zip_queue, None]) for _ in range(2)
]

for url, download_id in islice(master_exports(), 20):
    print(url)
