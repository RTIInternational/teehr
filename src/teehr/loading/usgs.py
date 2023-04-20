import utils
import config
import os
from typing import List, Union
from urllib import Path

from datetime import datetime, timedelta

from hydrotools.nwis_client.iv import IVDataService


def _fetch_usgs(
    sites: List[str],
    start_dt: str,
    end_dt: str
):
    # Retrieve data for all sites
    service = IVDataService(
        value_time_label="value_time",
        enable_cache=False
    )
    observations_data = service.get(
        sites=sites,
        startDT=start_dt,
        endDT=end_dt
    )

    # Return the data
    return observations_data


def _filter_to_hourly(df):
    """Filter out data not reported on the hour."""
    df.set_index("value_time", inplace=True)
    df2 = df[
        df.index.hour.isin(range(0, 23)) 
        & (df.index.minute == 0) 
        & (df.index.second == 0)
    ]
    df2.reset_index(level=0, allow_duplicates=True, inplace=True)
    return df2


def _filter_no_data(df, no_data_value=-999):
    """Filter out data not reported on the hour."""
    df2 = df[
        df[df["value"] != no_data_value]
    ]
    return df2

def 

def usgs_to_parquet(
    sites: List[str],
    start_dt: datetime,
    end_dt: datetime,
    output_dir: Union[str, Path],
    chunk_by: Union[str, None] = None,
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
):
    """Fetch USGS gage data and save as a Parquet file.
    
    Parameters
    ----------
    sites : List[str]
        List of USGS gages sites to fetch.
    start_dt : datetime
        Start time of fetch
    end_dt : datetime
        End time of fetch
    output_dir : str
    chunk_by : Union[str, None], default = None
        How to "chunk" the fetching and storing of the data.
        Valid options = ["day", "site", None]
    filter_to_hourly: bool = True
    filter_no_data: bool = True
    
    Returns
    -------
    
    """
    if chunk_by not in ["day", "site", None]:
        raise ValueError("chunk_by must be one of ['day', 'site', None]")
    
    # Fetch all at once
    if chunk_by is None:
        start_dt_str = start_dt.strftime("%Y-%m-%d")
        end_dt_str = end_dt.strftime("%Y-%m-%d")
        observations_df = fetch_usgs(
            sites=sites,
            start_dt=start_dt_str,
            end_dt=end_dt_str
        )
        if filter_to_hourly == True:
            observations_df = _filter_to_hourly(observations_df)
        if filter_no_data == True:
            observations_df = _filter_to_hourly(observations_df)
        return observations_df
    
    
    if chunk_by = "day":
        pass
    
    if chunk_by = "site":
        for site in sites:
            pass
    
    start = datetime(2023, 1, 1)
    download_period = timedelta(days=1)
    number_of_periods = 11

    sites = utils.get_usgs_gages()

    # Fetch USGS gage data in daily batches
    for p in range(number_of_periods):

        # Setup start and end date for fetch
        start_dt = (start + download_period * p)
        end_dt = (start + download_period * (p + 1))
        start_dt_str = start_dt.strftime("%Y-%m-%d")
        end_dt_str = end_dt.strftime("%Y-%m-%d")

        observations_data = fetch_usgs(
            sites=sites["gage_id"],
            start_dt=start_dt_str,
            end_dt=end_dt_str
        )

        
        # Save as parquet file
        parquet_filepath = os.path.join(config.USGS_PARQUET, f"{start_dt.strftime('%Y%m%d')}.parquet")
        utils.make_parent_dir(parquet_filepath)
        obs.to_parquet(parquet_filepath)


if __name__ == "__main__":
    ingest_usgs()