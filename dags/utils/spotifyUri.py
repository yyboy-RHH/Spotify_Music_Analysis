from utils.GCP_client import get_bq_client, get_storage_client
import pandas as pd
import logging
from google.cloud import bigquery
from typing import Union


def get_track_uris() -> pd.DataFrame:
    """
    get TrackUri from BigQuery
    """
    client = get_bq_client()
    query = """
    SELECT DISTINCT trackUri
    FROM `affable-hydra-422306-r3.stage_ChangeDataType.expand_table_2017_2024`
    """
    df = client.query(query).to_dataframe()
    logging.info(f"There are {len(df)} trackUris!")

    return df


#過濾已抓取的track_uri，從未抓取的track_uri繼續抓
def filter_track_uris(track_uris: list, last_track_uri: str) -> list:
    """
    filter TrackUri list
    """
    df_indexed = get_track_uris().set_index("trackUri")
    last_index = df_indexed.index.get_loc(last_track_uri)

    # Keep only the elements after the last_track_uri
    track_uris = df_indexed[last_index + 1 :]
    logging.info(f"Start from {last_index+1} of trackUris!")
    # print(track_uris.index.values.flatten().tolist())
    return track_uris.index.values.flatten().tolist()


def get_artist_uris() -> pd.DataFrame:
    """
    get ArtistUri from BigQuery
    """
    client = bigquery.Client()

    # query artistUri
    query = """
    SELECT DISTINCT artistUri
    FROM `affable-hydra-422306-r3.stage_ChangeDataType.expand_table_2017_2024`
    """
    df = client.query(query).to_dataframe()
    logging.info(f"There are {len(df)} artistUris!")

    return df  # return artistUri


#過濾已抓取的artist_uri，從未抓取的artist_uri繼續抓
def filter_artist_uris(artist_uris: list, last_artist_uri: str) -> list:
    """
    filter ArtistUri list
    """
    # Find the index of last_artist_uri in artist_uris
    df_indexed = get_artist_uris().set_index("artistUri")
    last_index = df_indexed.index.get_loc(last_artist_uri)

    # Keep only the elements after the last_artist_uri
    artist_uris = df_indexed[last_index + 1 :]
    logging.info(f"Start from {last_index+1} of artistUris!")
    # print(artist_uris.index.values.flatten().tolist())
    return artist_uris.index.values.flatten().tolist()


# 確任是否有缺失值
def check_missing_data(uri_type: str, data: list) -> bool:
    """
    if there is no missing data of API will return TRUE
    """

    if uri_type == "track":
        chart_uris = len(get_track_uris())

    elif uri_type == "artist":
        chart_uris = len(get_artist_uris())

    logging.info(f"download {len(data)} data from API, original data has {chart_uris}")
    return len(data) == chart_uris


# 如果有缺失值，把他找出來
def find_missing_data(uri_type: str, data: list) -> list:
    """
    find missing data
    """
    all_uris = []
    for item in data:
        if "uri" in item:
            all_uris.append(item["uri"].split(":")[-1])

    # print(all_uris)

    if uri_type == "track":
        chart_uris = get_track_uris().values.flatten().tolist()

    elif uri_type == "artist":
        chart_uris = get_artist_uris().values.flatten().tolist()

    diff = [uri for uri in chart_uris if uri not in all_uris]
    print(f"diff are {len(diff)}")
    return diff
