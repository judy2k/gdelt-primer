import csv
import io
import pathlib
import tempfile
from zipfile import ZipFile

import requests


def upload(zip_uri, mdb_uri, download_id):
    print(f"Downloading {zip_uri}")
    data = requests.get(zip_uri).content
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
