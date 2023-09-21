# %%
import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


token = os.environ.get("INFLUXDB_TOKEN")
org = "Ahtonen & Co."
url = "http://localhost:8086"
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket = "test"  # TODO race

write_api = client.write_api(write_options=SYNCHRONOUS)

# root logger config
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
# logger for this file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BOATS = Path(os.environ.get("ADRENA_COMPETITOR_FILE"))
# POSITIONS = Path(os.environ.get("EXPEDITION_POSITIONS"))
POSITIONS = Path(os.environ.get("ADRENA_POSITIONS"))
TZ = os.environ.get("TZ")
COLS = ["boat number", "lat", "lon", "timestamp"]


def parse_adrena_competitors(file: Path) -> pd.DataFrame:
    """Parse Adrena competitor info file."""

    df = pd.read_csv(
        file,
        encoding="latin-1",
        skiprows=1,
        header=None,
        sep=";",
        usecols=[0, 1, 7, 11],
        names=["boat name", "boat type", "boat number", "skipper name"],
    )

    return df


def parse_expedition_positions(file: Path) -> pd.DataFrame:
    """Parse Expedition format position file."""
    # check if download has failed
    # TODO this could be done higher up
    if file.read_text().startswith("<html>"):
        raise AssertionError

    df = pd.read_csv(file, header=None, sep=",", names=COLS)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%y%m%d%H%M")
    df["timestamp"] = df["timestamp"].dt.tz_localize(TZ)

    # df.set_index("boat number", inplace=True)

    return df


# %%
def latitude_string2float(s: str) -> float:
    if s.endswith("N"):
        return float(s[:-1])
    elif s.endswith("S"):
        return -float(s[:-1])
    raise ValueError(f"Unrecognised latitude in {s}")


def longitude_string2float(s: str) -> float:
    if s.endswith("E"):
        return float(s[:-1])
    elif s.endswith("W"):
        return -float(s[:-1])
    raise ValueError(f"Unrecognised longitude in {s}")


def parse_adrena_positions(file: Path) -> pd.DataFrame:
    """Parse Adrena format position file."""
    # TODO put higher level, same than with Expedition parson
    if file.read_text().startswith("<html>"):
        raise AssertionError

    df = pd.read_csv(
        file,
        skiprows=[0],
        usecols=[1, 2, 3, 4],
        header=None,
        sep=";",
        names=COLS,
        converters={"lat": latitude_string2float, "lon": longitude_string2float},
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%y %H:%M:%S")
    df["timestamp"] = df["timestamp"].dt.tz_localize(TZ)

    # df.set_index("boat number", inplace=True)

    return df


# %%
df_list = []

for file in POSITIONS.iterdir():
    try:
        df_list.append(parse_adrena_positions(file))
        #    df = parse_expedition_positions(file)
        logger.debug(file.name)
    #    logger.debug(df.dtypes)
    except AssertionError as e:
        logger.error("Invalid file: %s", file.name)

df = pd.concat(df_list)
logger.debug(df.shape)

# %%
df_boats = parse_adrena_competitors(BOATS)

# %%
boat_number_to_write = 50
calamity = (
    df[df["boat number"] == boat_number_to_write]
#    .drop("boat number", axis=1)
    .set_index("timestamp")
    .sort_index()
    .drop_duplicates()
)
tags = (
    df_boats[df_boats["boat number"] == boat_number_to_write]
    .transpose()
    .to_dict()
    .popitem()[1]
)
# populate tags
for t,v in tags.items():
    calamity[t] = v
# %%
write_api.write(
    bucket,
    org,
    record=calamity,
    data_frame_measurement_name="location3",
    data_frame_tag_columns=[
        'boat number',
        'boat name',
        'boat type',
        'skipper name',
    ]
)
# %%
