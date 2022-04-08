import gzip
import pathlib
import re


URI_MATCH = re.compile(
    r"http://data.gdeltproject.org/gdeltv2/(?P<download_id>\d+).export.CSV.zip"
)


def master_exports(path="master.txt.gz"):
    with gzip.open(path, "rt", encoding="utf-8") as master_file:
        master_lines = master_file.readlines()
    rows = [line.strip().split(" ") for line in reversed(master_lines)]
    for row in rows:
        _, _, uri = row
        if match := URI_MATCH.match(
            uri,
        ):
            download_id = match.group("download_id")
            yield (uri, download_id)


def old_import(db):
    path = pathlib.Path("master.txt.gz")
    if not path.exists():
        raise click.ClickException("Run the update command to download a master file.")

    for uri, download_id in master_exports(path):
        # Optimize by querying a batch of ids and doing an intersection.
        if not db.already_inserted(download_id):
            upload(uri, os.environ["MDB_URI"], download_id)
            db.convert(download_id)
            db.set_inserted(download_id)
