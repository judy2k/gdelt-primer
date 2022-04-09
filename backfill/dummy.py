from queue import Queue
import subprocess
import threading

from itertools import islice
import io
import os
import shutil
from zipfile import ZipFile

import requests

from gmloader.master import master_exports


def zip_downloader(input: Queue, output: Queue):
    print("Zip Downloader")
    while True:
        url = input.get()
        try:
            print(f"Downloading {url}")
            zip_data = requests.get(url).content
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
                        output.put(z.read(item))
        finally:
            input.task_done()


def upload(input: Queue, *, mongoimport_path=None, mdb_uri=os.getenv("MDB_URI")):
    if mongoimport_path is None:
        mongoimport_path = shutil.which("mongoimport")
    if mongoimport_path is None:
        raise Exception("mongoimport could not be found on the PATH.")

    # TODO: Locate/manage fields.txt automatically
    process = subprocess.Popen(
        [
            mongoimport_path,
            "--fieldFile=fields.txt",
            "--columnsHaveTypes",
            "--ignoreBlanks",
            "--type=tsv",
            "--drop",  # TODO: DO NOT LEAVE THIS HERE
            "--collection=loader_temp",
            mdb_uri,
        ],
        stdin=subprocess.PIPE,
    )

    while True:
        csv_bytes = input.get()
        print("Uploading block...")
        try:
            process.stdin.write(csv_bytes)
            print("Uploaded.")
        finally:
            input.task_done()


url_queue = Queue()
zip_queue = Queue(10)
upload_queue = Queue(5)

downloading_threads = [
    threading.Thread(target=zip_downloader, args=[url_queue, zip_queue])
    for _ in range(10)
]
unzipping_threads = [
    threading.Thread(target=zcat, args=[zip_queue, upload_queue]) for _ in range(2)
]

for t in downloading_threads + unzipping_threads:
    t.start()

for url, download_id in islice(master_exports(), 5):
    url_queue.put(url)
    print(f"Put {url}")
print("All urls submitted")

upload(upload_queue)
