import csv
import io
import zipfile
from datetime import datetime
from xml.etree import ElementTree

import requests

from src.db import connect


LISTING_URL = "https://s3.amazonaws.com/tripdata/"
OBJECT_URL = "https://s3.amazonaws.com/tripdata/{key}"


def _listing():
    response = requests.get(LISTING_URL, timeout=60)
    response.raise_for_status()

    root = ElementTree.fromstring(response.content)
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    return [
        {
            "key": item.find("s3:Key", namespace).text,
            "last_modified": item.find("s3:LastModified", namespace).text,
        }
        for item in root.findall("s3:Contents", namespace)
    ]


def _choose_object(market, window):
    year, month = window.split("-")
    compact = f"{year}{month}"

    objects = _listing()

    if market == "jc":
        matches = [
            obj for obj in objects
            if obj["key"].startswith(f"JC-{compact}")
            and obj["key"].endswith(".zip")
        ]
    else:
        matches = [
            obj for obj in objects
            if not obj["key"].startswith("JC-")
            and obj["key"].endswith(".zip")
            and (
                obj["key"].startswith(f"{year}-citibike")
                or obj["key"].startswith(f"{compact}-citibike")
            )
        ]

    if not matches:
        raise ValueError(f"no source object found for {market} {window}")

    return max(matches, key=lambda obj: obj["last_modified"])


def _csv_members(archive, market, window):
    year, month = window.split("-")

    members = [
        info for info in archive.infolist()
        if not info.is_dir()
        and info.filename.lower().endswith(".csv")
        and "__macosx" not in info.filename.lower()
    ]

    if market == "nyc" and year == "2018" and month == "04":
        preferred = [
            info for info in members
            if "4_April/" in info.filename
        ]
        if preferred:
            return preferred

    compact = f"{year}{month}"
    matching = [
        info for info in members
        if compact in info.filename.replace("-", "")
    ]

    return matching or members


def ingest(market, window):
    source = _choose_object(market, window)

    response = requests.get(
        OBJECT_URL.format(key=source["key"]),
        timeout=300,
    )
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        members = _csv_members(archive, market, window)

        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM bronze_objects WHERE market = %s AND window_key = %s",
                    (market, window),
                )

                for member in members:
                    raw = archive.read(member)

                    text = io.TextIOWrapper(
                        io.BytesIO(raw),
                        encoding="utf-8-sig",
                        errors="replace",
                        newline="",
                    )
                    row_count = sum(1 for _ in csv.reader(text)) - 1

                    member_timestamp = datetime(*member.date_time)

                    cur.execute(
                        """
                        INSERT INTO bronze_objects (
                            market,
                            window_key,
                            object_key,
                            member_name,
                            member_timestamp,
                            raw_bytes,
                            row_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            market,
                            window,
                            source["key"],
                            member.filename,
                            member_timestamp,
                            raw,
                            row_count,
                        ),
                    )
