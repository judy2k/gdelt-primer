#!/usr/bin/env python3

import csv
import enum
import subprocess
import gzip
import io
import os
import shutil
import tempfile
import re
from zipfile import ZipFile

import click
import pathlib
from pymongo import MongoClient
import requests

from .master import master_exports
from .mongoimport import upload


@click.group()
def main():
    pass


@main.command()
def test():
    db = DB(os.environ["MDB_URI"])
    download_id = "20220401151500"
    print(download_id, db.already_inserted(download_id))


@main.command()
def update():
    master_csv_data = requests.get(
        "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    ).text
    with gzip.open("master.txt.gz", "wt", encoding="utf-8") as master_file:
        master_file.write(master_csv_data)


@main.command()
def backfill():
    db = DB(os.environ["MDB_URI"])
    path = pathlib.Path("master.txt.gz")
    if not path.exists():
        raise click.ClickException("Run the update command to download a master file.")

    for uri, download_id in master_exports(path):
        if not db.already_inserted(download_id):
            upload(uri, os.environ["MDB_URI"], download_id)
            db.convert(download_id)
            db.set_inserted(download_id)


if __name__ == "__main__":
    main()
