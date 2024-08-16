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
from teehr.loading.timeseries import (
    validate_and_insert_timeseries,
)
from teehr.evaluation.utils import (
    get_schema_variable_name,
)
from teehr.models.fetching.nwm22_grid import ForcingVariablesEnum
from teehr.models.fetching.utils import (
    USGSChunkByEnum,
    USGSServiceEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum,
    NWMChunkByEnum,
    ChannelRtRetroVariableEnum,
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.fetching.const import (
    USGS_CONFIGURATION_NAME,
    USGS_VARIABLE_MAPPER,
    VARIABLE_NAME,
    NWM_VARIABLE_MAPPER
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
            const.FETCHING_CACHE_DIR,
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
        service: USGSServiceEnum = "iv",
        filter_to_hourly: bool = True,
        filter_no_data: bool = True,
        convert_to_si: bool = True,
        overwrite_output: Optional[bool] = False,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """Fetch USGS gage data and save as a Parquet file."""
        logger.info("Getting primary location IDs.")
        if sites is None:
            locations = self.eval.query.get_locations_table()
            sites = locations["id"].str.removeprefix("usgs-").to_list()

        usgs_variable_name = USGS_VARIABLE_MAPPER[VARIABLE_NAME][service]

        # TODO: Get timeseries_type from the configurations table?

        usgs_to_parquet(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.usgs_cache_dir,
                USGS_CONFIGURATION_NAME,
                usgs_variable_name
            ),
            chunk_by=chunk_by,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si,
            overwrite_output=overwrite_output
        )

        validate_and_insert_timeseries(
            in_path=Path(
                self.usgs_cache_dir,
                USGS_CONFIGURATION_NAME,
                usgs_variable_name
            ),
            dataset_path=self.eval.dataset_dir,
            timeseries_type=timeseries_type,
        )

    def nwm_retrospective_points(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: ChannelRtRetroVariableEnum,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        location_ids: Optional[List[int]] = None,
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        timeseries_type: TimeseriesTypeEnum = "secondary"
    ):
        """Fetch NWM retrospective at NWM COMIDs and store as Parquet file."""
        configuration = f"{nwm_version}_retrospective"
        schema_variable_name = get_schema_variable_name(variable_name)

        # TODO: Get timeseries_type from the configurations table?

        logger.info("Getting secondary location IDs.")
        # NOTE: Does this depend on timeseries_type? This will be a method on
        # crosswalk table component class.
        if location_ids is None:
            locations = self.eval.query.get_locations_table()
            primary_location_ids = locations["id"].to_list()
            secondary_location_ids = self.eval.query.\
                get_secondary_location_ids(
                    primary_location_ids=primary_location_ids
                )
            location_ids = secondary_location_ids.secondary_location_id.\
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
                schema_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            in_path=Path(
                self.nwm_cache_dir,
                configuration,
                schema_variable_name
            ),
            dataset_path=self.eval.dataset_dir,
            timeseries_type=timeseries_type,
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
        location_id_prefix: Optional[Union[str, None]] = None,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """Compute the weighted average for NWM gridded data."""
        configuration = f"{nwm_version}_retrospective"
        schema_variable_name = get_schema_variable_name(variable_name)

        # TODO: Get timeseries_type from the configurations table?

        nwm_retro_grids_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            zonal_weights_filepath=zonal_weights_filepath,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                configuration,
                schema_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            in_path=Path(
                self.nwm_cache_dir,
                configuration,
                schema_variable_name
            ),
            dataset_path=self.eval.dataset_dir,
            timeseries_type=timeseries_type,
        )

    def nwm_forecast_points(
        self,
        configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        nwm_version: SupportedNWMOperationalVersionsEnum,
        location_ids: Optional[List[int]] = None,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        t_minus_hours: Optional[List[int]] = None,
        process_by_z_hour: Optional[bool] = True,
        stepsize: Optional[int] = 100,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        timeseries_type: TimeseriesTypeEnum = "secondary"
    ):
        """Fetch NWM point data and save as a Parquet file in TEEHR format.""" # noqa
        logger.info("Getting primary location IDs.")
        # NOTE: Does this depend on timeseries_type? This will be a method on
        # crosswalk table component class.
        if location_ids is None:
            locations = self.eval.query.get_locations_table()
            primary_location_ids = locations["id"].to_list()
            secondary_location_ids = self.eval.query.\
                get_secondary_location_ids(
                    primary_location_ids=primary_location_ids
                )
            location_ids = secondary_location_ids.secondary_location_id.\
                str.removeprefix(f"{nwm_version}-").to_list()

        # TODO: Read timeseries_type from the configurations table?

        schema_variable_name = get_schema_variable_name(variable_name)
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
                schema_variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            process_by_z_hour=process_by_z_hour,
            stepsize=stepsize,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            in_path=Path(
                self.nwm_cache_dir,
                configuration,
                schema_variable_name
            ),
            dataset_path=self.eval.dataset_dir,
            timeseries_type=timeseries_type,
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
        location_id_prefix: Optional[Union[str, None]] = None,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """
        Fetch NWM gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, convert
        and save to TEEHR tabular format.
        """ # noqa

        # TODO: Get timeseries_type from the configurations table?

        schema_variable_name = get_schema_variable_name(variable_name)
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
                schema_variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        pass

        validate_and_insert_timeseries(
            in_path=Path(
                self.nwm_cache_dir,
                configuration,
                schema_variable_name
            ),
            dataset_path=self.eval.dataset_dir,
            timeseries_type=timeseries_type,
        )
