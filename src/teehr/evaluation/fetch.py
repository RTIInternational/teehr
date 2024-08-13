"""Component class for fetching data from external sources."""
from typing import Union, List, Optional
from datetime import datetime
import logging
from pathlib import Path

import pandas as pd

import teehr.const as const
from teehr.fetching.usgs.usgs import usgs_to_parquet
from teehr.fetching.nwm.nwm_points import nwm_to_parquet
from teehr.fetching.nwm.nwm_grids import nwm_grids_to_parquet
from teehr.fetching.nwm.retrospective_points import nwm_retro_to_parquet
from teehr.fetching.nwm.retrospective_grids import nwm_retro_grids_to_parquet
from teehr.models.fetching.nwm22_grid import ForcingVariablesEnum
from teehr.models.fetching.utils import (
    USGSChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum,
    NWMChunkByEnum,
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod
)
from teehr.fetching.const import (
    USGS_CONFIGURATION_NAME,
    USGS_VARIABLE_NAME
)

logger = logging.getLogger(__name__)


class Fetch:
    """Component class for fetching data from external sources."""

    def __init__(self, eval) -> None:
        """Initialize the Fetch class."""
        # Now we have access to the Evaluation object.
        self.eval = eval
        self.usgs_cache_dir = Path(
            eval.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.USGS_CACHE_DIR,
        )
        self.nwm_cache_dir = Path(
            eval.cache_dir,
            const.NWM_CACHE_DIR
        )
        self.kerchunk_cache_dir = Path(
            eval.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.KERCHUNK_DIR
        )
        self.weights_cache_dir = Path(
            eval.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.WEIGHTS_DIR
        )

    def usgs_streamflow(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        sites: Optional[List[str]] = None,
        chunk_by: Union[USGSChunkByEnum, None] = None,
        filter_to_hourly: bool = True,
        filter_no_data: bool = True,
        convert_to_si: bool = True,
        overwrite_output: Optional[bool] = False
    ):
        """Fetch USGS gage data and save as a Parquet file."""
        logger.info("Fetching USGS streamflow data.")
        if sites is None:
            locations = self.eval.query.get_locations()
            sites = locations["id"].str.removeprefix("usgs-").to_list()

        usgs_to_parquet(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.usgs_cache_dir,
                USGS_CONFIGURATION_NAME,
                USGS_VARIABLE_NAME
            ),
            chunk_by=chunk_by,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si,
            overwrite_output=overwrite_output
        )

        self.eval.load.import_primary_timeseries(
            in_path=Path(
                self.usgs_cache_dir,
                USGS_CONFIGURATION_NAME,
                USGS_VARIABLE_NAME
            )
        )

    def nwm_retrospective_points(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        location_ids: Optional[List[int]] = None,
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS"
    ):
        """Fetch NWM retrospective at NWM COMIDs and store as Parquet file."""
        logger.info("Fetching NWM retrospective point data.")

        configuration = f"{nwm_version}_retrospective"

        if location_ids is None:
            crosswalk = self.eval.query.get_crosswalk()
            location_ids = crosswalk["secondary_location_id"]. \
                str.removeprefix(f"{nwm_version}-").to_list()

        nwm_retro_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                configuration,
                variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain
        )
        # TODO: Do we need a mapper for all possible unit names and
        # variable names so they conform to our schema? Or just manually
        # add them all to the csv.
        self.eval.load.import_secondary_timeseries(
            in_path=Path(
                self.nwm_cache_dir,
                configuration,
                variable_name
            )
        )

    def nwm_retrospective_grids(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: ForcingVariablesEnum,
        zonal_weights_filepath: Union[str, Path],
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        location_id_prefix: Optional[Union[str, None]] = None
    ):
        """Compute the weighted average for NWM gridded data."""
        logger.info("Fetching NWM retrospective grid data.")
        configuration = f"{nwm_version}_retrospective"
        nwm_retro_grids_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            zonal_weights_filepath=zonal_weights_filepath,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                configuration,
                variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            location_id_prefix=location_id_prefix
        )

    def nwm_forecast_points(
        self,
        configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        location_ids: List[int],
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        t_minus_hours: Optional[List[int]] = None,
        process_by_z_hour: Optional[bool] = True,
        stepsize: Optional[int] = 100,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False
    ):
        """Fetch NWM point data and save as a Parquet file in TEEHR format.""" # noqa
        logger.info("Fetching NWM forecast point data.")
        nwm_to_parquet(
            configuration=configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            location_ids=location_ids,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                configuration,
                variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            process_by_z_hour=process_by_z_hour,
            stepsize=stepsize,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output
        )

    def nwm_forecast_grids(
        self,
        configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        zonal_weights_filepath: Union[Path, str],
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        t_minus_hours: Optional[List[int]] = None,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        location_id_prefix: Optional[Union[str, None]] = None
    ):
        """
        Fetch NWM gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, convert
        and save to TEEHR tabular format.
        """ # noqa
        logger.info("Fetching NWM forecast grid data.")
        nwm_grids_to_parquet(
            configuration=configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            zonal_weights_filepath=zonal_weights_filepath,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                configuration,
                variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix
        )
