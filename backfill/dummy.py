from queue import Queue, Empty, Full
import subprocess
import threading
from threading import Event

from itertools import islice
import io
import os
import shutil
import time
from zipfile import ZipFile

import requests

from gmloader.master import master_exports


def zip_downloader(input: Queue, output: Queue, shutdown_event: Event):
    print("Zip Downloader")
    while not shutdown_event.is_set():
        try:
            url = input.get(timeout=0.1)
        except Empty:
            continue
        try:
            print(f"Downloading {url}")
            zip_data = requests.get(url).content
            print(f"Downloaded  {url}")
            while True:
                try:
                    output.put(zip_data, timeout=0.1)
                    break
                except Full:
                    pass

        finally:
            input.task_done()

    # Drain the input queue:
    try:
        while input.not_empty:
            input.get_nowait()
    except Empty:
        pass

    print("Zip downloader closed.")


def zcat(input: Queue, output: Queue, shutdown_event: Event):
    while not shutdown_event.is_set():
        try:
            data = input.get(timeout=0.1)
        except Empty:
            continue
        try:
            with ZipFile(io.BytesIO(data)) as z:
                zip_items = z.infolist()
                for item in zip_items:
                    if item.filename.endswith("CSV"):
                        output.put(z.read(item))
        finally:
            input.task_done()

    # Drain the input queue:
    try:
        while input.not_empty:
            input.get_nowait()
    except Empty:
        pass
    print("Zcat closed.")


def upload(
    input: Queue,
    shutdown_event: Event,
    *,
    mongoimport_path=None,
    mdb_uri=os.getenv("MDB_URI"),
):
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
        start_new_session=True,
    )

    while not shutdown_event.is_set():
        try:
            csv_bytes = input.get(timeout=0.1)
        except Empty:
            continue
        print("Uploading block...")
        try:
            process.stdin.write(csv_bytes)
            print("Uploaded.")
        except BrokenPipeError:
            # If we're shutting down, that's the reason for the broken pipe.
            # If not, it's a real error.
            if not shutdown_event.is_set():
                raise
        finally:
            input.task_done()

    process.kill()
    # Drain the input queue:
    try:
        while input.not_empty:
            input.get_nowait()
    except Empty:
        pass
    print("Uploader closed.")


def main():
    # TODO: Refactor the following into a function:
    shutdown = Event()

    url_queue = Queue()
    zip_queue = Queue(10)
    upload_queue = Queue(5)

    downloading_threads = [
        threading.Thread(target=zip_downloader, args=[url_queue, zip_queue, shutdown])
        for _ in range(10)
    ]
    unzipping_threads = [
        threading.Thread(target=zcat, args=[zip_queue, upload_queue, shutdown])
        for _ in range(2)
    ]

    for url, _ in islice(master_exports(), 5):
        url_queue.put(url)
        print(f"Put {url}")
    print("All urls submitted")

    upload_thread = threading.Thread(
        target=upload,
        args=[upload_queue, shutdown],
    )

    all_threads = downloading_threads + unzipping_threads + [upload_thread]
    for t in all_threads:
        t.start()

    try:
        print("----------------")
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("!!!!!!!!!!!!!")
        shutdown.set()
        print("Awaiting clean thread shutdown...")
        for t in all_threads:
            t.join()
        print("done!")


if __name__ == "__main__":
    main()
