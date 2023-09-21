#%%
import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# root logger config
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
# logger for this file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BOATS = Path(os.environ.get("ADRENA_COMPETITOR_FILE"))
#POSITIONS = Path(os.environ.get("EXPEDITION_POSITIONS"))
POSITIONS = Path(os.environ.get("ADRENA_POSITIONS"))
TZ = os.environ.get("TZ")
COLS = ["boat number", "lat", "lon", "timestamp"]

def parse_adrena_competitors(file: Path) -> pd.DataFrame:
    """Parse Adrena competitor info file."""

    df = pd.read_csv(file,
        encoding="latin-1",
        skiprows=1,
        header=None,
        sep=';',
        usecols=[0, 1, 7, 11],
        names=['boat name', 'boat type', 'boat number', 'skipper name']
    )

    return df

def parse_expedition_positions(file: Path) -> pd.DataFrame:
    """Parse Expedition format position file."""
    # check if download has failed
    # TODO this could be done higher up
    if file.read_text().startswith('<html>'):
        raise AssertionError
    
    df = pd.read_csv(file,
        header=None,
        sep=',',
        names=COLS
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%y%m%d%H%M")
    df['timestamp'] = df['timestamp'].dt.tz_localize(TZ)

    df.set_index("boat number", inplace=True)

    return df
#%%
def latitude_string2float(s: str) -> float:
    if s.endswith('N'):
        return float(s[:-1])
    elif s.endswith('S'):
        return -float(s[:-1])
    raise ValueError(f'Unrecognised latitude in {s}')

def longitude_string2float(s: str) -> float:
    if s.endswith('E'):
        return float(s[:-1])
    elif s.endswith('W'):
        return -float(s[:-1])
    raise ValueError(f'Unrecognised longitude in {s}')

def parse_adrena_positions(file: Path) -> pd.DataFrame:
    """Parse Adrena format position file."""
    # TODO put higher level, same than with Expedition parson 
    if file.read_text().startswith('<html>'):
        raise AssertionError
    
    df = pd.read_csv(file,
        skiprows=[0],
        usecols=[1,2,3,4],
        header=None,
        sep=';',
        names=COLS,
        converters={"lat": latitude_string2float, "lon":longitude_string2float}
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%m/%d/%y %H:%M:%S")
    df['timestamp'] = df['timestamp'].dt.tz_localize(TZ)

    df.set_index("boat number", inplace=True)
    
    return df

#%%
for file in POSITIONS.iterdir():
    df = parse_adrena_positions(file)
#    df = parse_expedition_positions(file)

    logger.debug(df)
    logger.debug(df.dtypes)

    break

#%%
logger.debug(parse_adrena_competitors(BOATS))

    
# %%
