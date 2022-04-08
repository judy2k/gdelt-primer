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

URI_MATCH = re.compile(
    r"http://data.gdeltproject.org/gdeltv2/(?P<download_id>\d+).export.CSV.zip"
)


class DB:
    def __init__(self, uri):
        self.db = MongoClient(uri).get_default_database()

    def already_inserted(self, download_id):
        """Return True if the download_id is already uploaded."""
        return (
            self.db.get_collection("insertLog").find_one(
                {
                    "_id": download_id,
                }
            )
            is not None
        )

    def set_inserted(self, download_id):
        return self.db.get_collection("insertLog").insert_one(
            {
                "_id": download_id,
            }
        )

    def convert(self, download_id):
        self.db.get_collection("eventsCSV").aggregate(
            [
                {
                    "$match": {"downloadId": download_id},
                },
                {
                    "$project": {
                        "GlobalEventId": 1,
                        "Day": 1,
                        "MonthYear": 1,
                        "Year": 1,
                        "FractionDate": 1,
                        "Actor1": {
                            "Code": "$Actor1Code",
                            "Name": "$Actor1Name",
                            "CountryCode": "$Actor1CountryCode",
                            "KnownGroupCode": "$Actor1KnownGroupCode",
                            "EthnicCode": "$Actor1EthnicCode",
                            "Religion1Code": "$Actor1Religion1Code",
                            "Religion2Code": "$Actor1Religion2Code",
                            "Type1Code": "$Actor1Type1Code",
                            "Type2Code": "$Actor1Type2Code",
                            "Type3Code": "$Actor1Type3Code",
                            "Geo_Type": "$Actor1Geo_Type",
                            "Geo_Fullname": "$Actor1Geo_Fullname",
                            "Geo_CountryCode": "$Actor1Geo_CountryCode",
                            "Geo_ADM1Code": "$Actor1Geo_ADM1Code",
                            "Geo_ADM2Code": "$Actor1Geo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$Actor1Geo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$Actor1Geo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$Actor1Geo_FeatureID",
                        },
                        "Actor2": {
                            "Code": "$Actor2Code",
                            "Name": "$Actor2Name",
                            "CountryCode": "$Actor2CountryCode",
                            "KnownGroupCode": "$Actor2KnownGroupCode",
                            "EthnicCode": "$Actor2EthnicCode",
                            "Religion1Code": "$Actor2Religion1Code",
                            "Religion2Code": "$Actor2Religion2Code",
                            "Type1Code": "$Actor2Type1Code",
                            "Type2Code": "$Actor2Type2Code",
                            "Type3Code": "$Actor2Type3Code",
                            "Geo_Type": "$Actor2Geo_Type",
                            "Geo_Fullname": "$Actor2Geo_Fullname",
                            "Geo_CountryCode": "$Actor2Geo_CountryCode",
                            "Geo_ADM1Code": "$Actor2Geo_ADM1Code",
                            "Geo_ADM2Code": "$Actor2Geo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$Actor2Geo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$Actor2Geo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$Actor2Geo_FeatureID",
                        },
                        "Action": {
                            "Geo_Type": "$ActionGeo_Type",
                            "Geo_Fullname": "$ActionGeo_Fullname",
                            "Geo_CountryCode": "$ActionGeo_CountryCode",
                            "Geo_ADM1Code": "$ActionGeo_ADM1Code",
                            "Geo_ADM2Code": "$ActionGeo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$ActionGeo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$ActionGeo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$ActionGeo_FeatureID",
                        },
                        "IsRootEvent": 1,
                        "EventCode": 1,
                        "EventBaseCode": 1,
                        "EventRootCode": 1,
                        "QuadClass": 1,
                        "GoldsteinScale": 1,
                        "NumMentions": 1,
                        "NumSources": 1,
                        "NumArticles": 1,
                        "AvgTone": 1,
                        "internal": {"downloadId": "$downloadId"},
                    },
                },
                {
                    "$merge": {"into": "recentEvents"},
                },
            ]
        )


def upload(zip_uri, mdb_uri, download_id):
    print(f"Downloading {zip_uri}")
    data = requests.get(zip_uri).content
    tempfile.mktemp
    with ZipFile(io.BytesIO(data)) as z:
        zip_items = z.infolist()
        for item in zip_items:
            if item.filename.endswith("CSV"):
                print(f"Extracting file {item.filename}")
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = pathlib.Path(temp_dir)
                    z.extract(item, path=temp_dir)
                    extracted_file = temp_path / item.filename
                    converted_file = temp_path / (item.filename + "_converted")
                    with open(extracted_file, "r") as extracted_in, open(
                        converted_file, "w"
                    ) as converted_out:
                        csv_out = csv.writer(converted_out, dialect="excel-tab")
                        for row in csv.reader(extracted_in, dialect="excel-tab"):
                            csv_out.writerow(row + [download_id])
                    subprocess.run(
                        [
                            "mongoimport",
                            "--fieldFile=fields.txt",
                            "--columnsHaveTypes",
                            "--ignoreBlanks",
                            "--type=tsv",
                            "--collection=eventsCSV",
                            mdb_uri,
                            str(converted_file),
                        ]
                    )


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
    if not pathlib.Path("master.txt.gz").exists():
        raise click.ClickException("Run the update command to download a master file.")

    with gzip.open("master.txt.gz", "rt", encoding="utf-8") as master_file:
        master_lines = master_file.readlines()
    master_lines.reverse()
    rows = [line.strip().split(" ") for line in master_lines]
    for row in rows:
        size, hash, uri = row
        if match := URI_MATCH.match(
            uri,
        ):
            # Optimize by querying a batch of ids and doing an intersection.
            download_id = match.group("download_id")
            if not db.already_inserted(download_id):
                upload(uri, os.environ["MDB_URI"], download_id)
                db.convert(download_id)
                db.set_inserted(download_id)


if __name__ == "__main__":
    main()
