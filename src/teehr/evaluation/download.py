"""Component class for fetching data from external sources."""
from typing import Union, List, Optional
from datetime import datetime
import logging
from io import BytesIO

import pandas as pd
import geopandas as gpd
import requests

from teehr.loading.teehr_api import teehr_api_timeseries_to_dataframe

logger = logging.getLogger(__name__)


def _format_datetime_range(
    start_date: Union[str, datetime, pd.Timestamp, None] = None,
    end_date: Union[str, datetime, pd.Timestamp, None] = None
) -> Optional[str]:
    """Format start and end dates into ISO 8601 datetime range string.

    Parameters
    ----------
    start_date : Union[str, datetime, pd.Timestamp, None], optional
        Start date/time. If None and end_date is provided, returns "../end_date".
        If both None, returns None.
    end_date : Union[str, datetime, pd.Timestamp, None], optional
        End date/time. If None and start_date is provided, returns "start_date/..".

    Returns
    -------
    Optional[str]
        ISO 8601 datetime range string (e.g., "2020-01-01/2020-12-31")
        or open-ended range (e.g., "2020-01-01/.." or "../2020-12-31")
        or None if both dates are None
    """
    if start_date is None and end_date is None:
        return None

    if start_date is None:
        end_dt = pd.Timestamp(end_date)
        return f"../{end_dt.isoformat()}"

    start_dt = pd.Timestamp(start_date)

    if end_date is None:
        return f"{start_dt.isoformat()}/.."

    end_dt = pd.Timestamp(end_date)
    return f"{start_dt.isoformat()}/{end_dt.isoformat()}"


class Download:
    """Component class for downloading data from the TEEHR warehouse."""

    def __init__(self, ev) -> None:
        """Initialize the Download class.

        Parameters
        ----------
        ev : Evaluation
            The parent Evaluation instance providing access to tables,
            Spark session, and cache directories.
        """
        self._ev = ev
        self._load = ev.load
        self.api_base_url = "https://api.teehr.rtiamanzi.org"
        self.verify_ssl = True

    def configure(
        self,
        api_base_url: str = None,
        api_port: int = None,
        verify_ssl: bool = True
    ) -> "Download":
        """Configure the warehouse API connection settings.

        Parameters
        ----------
        api_base_url : str, optional
            Base URL for the TEEHR warehouse API.
            Default: "https://api.teehr.rtiamanzi.org"
        api_port : int, optional
            Port number for the API. If provided, will be appended to the
            base URL (e.g., "https://api.teehr.rtiamanzi.org:8443").
        verify_ssl : bool, optional
            Whether to verify SSL certificates when making requests.
            Default: True

        Returns
        -------
        Download
            Returns self for method chaining

        Examples
        --------
        >>> ev.download.configure(
        ...     api_base_url="https://api.teehr.rtiamanzi.org",
        ...     api_port=8443,
        ...     verify_ssl=True
        ... )
        >>> locations = ev.download.locations(prefix="usgs")
        """
        base_url = api_base_url or "https://api.teehr.rtiamanzi.org"
        if api_port is not None:
            if "://" in base_url:
                scheme, rest = base_url.split("://", 1)
                if "/" in rest:
                    host, path = rest.split("/", 1)
                    base_url = f"{scheme}://{host}:{api_port}/{path}"
                else:
                    base_url = f"{scheme}://{rest}:{api_port}"
            else:
                base_url = f"{base_url}:{api_port}"
        self.api_base_url = base_url
        self.verify_ssl = verify_ssl

        logger.info(f"Download API configured: {self.api_base_url}")
        return self

    @staticmethod
    def _make_request(
        endpoint: str,
        api_base_url: str,
        verify_ssl: bool = False,
        params: dict = None
    ) -> requests.Response:
        """Make a request to the warehouse API.

        Parameters
        ----------
        endpoint : str
            API endpoint path (e.g., "collections/locations/items")
        api_base_url : str
            Base URL for the TEEHR warehouse API
        verify_ssl : bool, optional
            Whether to verify SSL certificates. Default: False
        params : dict, optional
            Query parameters for the request

        Returns
        -------
        requests.Response
            Response object from the API

        Raises
        ------
        requests.HTTPError
            If the request fails
        requests.exceptions.Timeout
            If the request times out
        """
        url = f"{api_base_url}/{endpoint}"

        logger.debug(f"Making request to {url} with params {params}")
        try:
            response = requests.get(url, params=params or {}, verify=verify_ssl, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(f"Request to {url} timed out.")
            raise

        return response

    def locations(
        self,
        prefix: str = None,
        bbox: List[float] = None,
        include_attributes: bool = False,
        limit: int = 10000,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[gpd.GeoDataFrame, None]:
        """Fetch locations from the warehouse API as a GeoDataFrame.

        Parameters
        ----------
        prefix : str, optional
            Filter locations by ID prefix (e.g., "usgs", "nwm30")
        bbox : list of float, optional
            Bounding box to filter locations by spatial extent,
            in the format [minx, miny, maxx, maxy].
        include_attributes : bool, optional
            Whether to include location attributes in the response.
            Default: False
        limit : int, optional
            Maximum number of locations to return. Default: 10000
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "locations" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[gpd.GeoDataFrame, None]
            GeoDataFrame containing locations with geometry, or None if load=True

        Examples
        --------
        >>> # Fetch USGS locations with attributes
        >>> locations = ev.download.locations(
        ...     prefix="usgs",
        ...     include_attributes=True
        ... )
        >>> # Fetch and load into local evaluation
        >>> locations = ev.download.locations(
        ...     prefix="usgs",
        ...     load=True
        ... )
        """
        params = {
            "limit": limit,
            **kwargs
        }

        if prefix:
            params["prefix"] = prefix
        if include_attributes:
            params["include_attributes"] = "true"
        if bbox:
            params["bbox"] = ",".join(map(str, bbox))

        response = self._make_request(
            "collections/locations/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        gdf = gpd.read_file(BytesIO(response.content))

        logger.info(f"Fetched {len(gdf)} locations from warehouse API")
        if load:
            if include_attributes:
                logger.warning(
                    "When load=True and include_attributes=True, "
                    "location attribute fields will be dropped when loading the locations table. "
                    "To return a dataframe with the location attribute fields included, "
                    "set load=False and include_attributes=True."
                )
            self._load.dataframe(
                df=gdf,
                table_name="locations",
                write_mode=write_mode
            )
            return
        return gdf

    def attributes(
        self,
        name: str = None,
        type: str = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch attributes from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by attribute name
        type : str, optional
            Filter by attribute type ("categorical" or "continuous")
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "attributes" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing attribute definitions, or None if load=True

        Examples
        --------
        >>> # Fetch all categorical attributes
        >>> attrs = ev.download.attributes(type="categorical")
        >>> # Fetch and load into local evaluation
        >>> attrs = ev.download.attributes(load=True)
        """
        params = {**kwargs}

        if name:
            params["name"] = name
        if type:
            params["type"] = type

        response = self._make_request(
            "collections/attributes/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} attributes from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="attributes",
                write_mode=write_mode
            )
            return
        return df

    def location_attributes(
        self,
        location_id: Union[str, List[str]] = None,
        attribute_name: str = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch location attributes from the warehouse API.

        Parameters
        ----------
        location_id : str or list of str, optional
            Filter by location ID(s)
        attribute_name : str, optional
            Filter by attribute name
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "location_attributes" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing location attribute values, or None if load=True

        Examples
        --------
        >>> # Fetch attributes for specific locations
        >>> loc_ids = ["usgs-01010000", "usgs-01010500"]
        >>> loc_attrs = ev.download.location_attributes(
        ...     location_id=loc_ids
        ... )
        >>> # Fetch and load into local evaluation
        >>> loc_attrs = ev.download.location_attributes(load=True)
        """
        params = {**kwargs}

        if location_id:
            params["location_id"] = location_id
        if attribute_name:
            params["attribute_name"] = attribute_name

        response = self._make_request(
            "collections/location_attributes/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} location attributes from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="location_attributes",
                write_mode=write_mode
            )
            return
        return df

    def units(
        self,
        name: str = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch units from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by unit name
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "units" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing unit definitions, or None if load=True

        Examples
        --------
        >>> # Fetch all units
        >>> units = ev.download.units()
        >>> # Fetch and load into local evaluation
        >>> units = ev.download.units(load=True)
        """
        params = {**kwargs}

        if name:
            params["name"] = name

        response = self._make_request(
            "collections/units/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} units from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="units",
                write_mode=write_mode
            )
            return
        return df

    def variables(
        self,
        name: str = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch variables from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by variable name
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "variables" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing variable definitions, or None if load=True

        Examples
        --------
        >>> # Fetch all variables
        >>> variables = ev.download.variables()
        >>> # Fetch and load into local evaluation
        >>> variables = ev.download.variables(load=True)
        """
        params = {**kwargs}

        if name:
            params["name"] = name

        response = self._make_request(
            "collections/variables/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} variables from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="variables",
                write_mode=write_mode
            )
            return
        return df

    def configurations(
        self,
        name: str = None,
        type: str = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch configurations from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by configuration name
        type : str, optional
            Filter by configuration type ("primary" or "secondary")
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "configurations" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing configuration definitions, or None if load=True

        Examples
        --------
        >>> # Fetch all primary configurations
        >>> configs = ev.download.configurations(type="primary")
        >>> # Fetch and load into local evaluation
        >>> configs = ev.download.configurations(load=True)
        """
        params = {**kwargs}

        if name:
            params["name"] = name
        if type:
            params["type"] = type

        response = self._make_request(
            "collections/configurations/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} configurations from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="configurations",
                write_mode=write_mode
            )
            return
        return df

    def location_crosswalks(
        self,
        primary_location_id: Union[str, List[str]] = None,
        secondary_location_id: Union[str, List[str]] = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch location crosswalks from the warehouse API.

        Parameters
        ----------
        primary_location_id : str or list of str, optional
            Filter by primary location ID(s)
        secondary_location_id : str or list of str, optional
            Filter by secondary location ID(s)
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "location_crosswalks" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame containing location crosswalk mappings, or None if load=True

        Examples
        --------
        >>> # Fetch crosswalks for specific primary locations
        >>> loc_ids = ["usgs-01010000", "usgs-01010500"]
        >>> crosswalks = ev.download.location_crosswalks(
        ...     primary_location_id=loc_ids
        ... )
        >>> # Fetch and load into local evaluation
        >>> crosswalks = ev.download.location_crosswalks(load=True)
        """
        params = {**kwargs}

        if primary_location_id:
            params["primary_location_id"] = primary_location_id
        if secondary_location_id:
            params["secondary_location_id"] = secondary_location_id

        response = self._make_request(
            "collections/location_crosswalks/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = pd.DataFrame(response.json()["items"])

        logger.info(f"Fetched {len(df)} location crosswalks from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="location_crosswalks",
                write_mode=write_mode
            )
            return
        return df

    def primary_timeseries(
        self,
        primary_location_id: Union[str, List[str]],
        configuration_name: Union[str, List[str]],
        variable_name: str = None,
        start_date: Union[str, datetime, pd.Timestamp] = None,
        end_date: Union[str, datetime, pd.Timestamp] = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch primary timeseries from the warehouse API.

        Parameters
        ----------
        primary_location_id : str or list of str
            Filter by primary location ID(s)
        configuration_name : str or list of str
            Filter by configuration name(s)
        variable_name : str, optional
            Filter by variable name
        start_date : Union[str, datetime, pd.Timestamp], optional
            Start date for timeseries query.
            Accepts ISO 8601 string, datetime, or pd.Timestamp.
        end_date : Union[str, datetime, pd.Timestamp], optional
            End date for timeseries query.
            Accepts ISO 8601 string, datetime, or pd.Timestamp.
            If None, only start_date is used.
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "primary_timeseries" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame with TEEHR primary timeseries schema, or None if load=True

        Examples
        --------
        >>> # Fetch USGS observations for specific locations and date range
        >>> timeseries = ev.download.primary_timeseries(
        ...     primary_location_id=["usgs-01010000", "usgs-01010500"],
        ...     configuration_name="usgs_observations",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02"
        ... )
        >>> # Fetch and load into local evaluation
        >>> timeseries = ev.download.primary_timeseries(
        ...     primary_location_id=["usgs-01010000"],
        ...     configuration_name="usgs_observations",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02",
        ...     load=True
        ... )
        """
        params = {**kwargs}

        if primary_location_id:
            params["primary_location_id"] = primary_location_id
        if configuration_name:
            params["configuration_name"] = configuration_name
        if variable_name:
            params["variable_name"] = variable_name

        datetime_range = _format_datetime_range(start_date, end_date)
        if datetime_range:
            params["datetime"] = datetime_range

        response = self._make_request(
            "collections/primary_timeseries/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = teehr_api_timeseries_to_dataframe(response.json())

        logger.info(f"Fetched {len(df)} primary timeseries values from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="primary_timeseries",
                write_mode=write_mode
            )
            return
        return df

    def secondary_timeseries(
        self,
        primary_location_id: Union[str, List[str]] = None,
        secondary_location_id: Union[str, List[str]] = None,
        configuration_name: Union[str, List[str]] = None,
        variable_name: str = None,
        start_date: Union[str, datetime, pd.Timestamp] = None,
        end_date: Union[str, datetime, pd.Timestamp] = None,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch secondary timeseries from the warehouse API.

        Parameters
        ----------
        primary_location_id : str or list of str, optional
            Filter by primary location ID(s).
            Either primary_location_id or secondary_location_id must be provided.
        secondary_location_id : str or list of str, optional
            Filter by secondary location ID(s).
            Either primary_location_id or secondary_location_id must be provided.
        configuration_name : str or list of str, optional
            Filter by configuration name(s)
        variable_name : str, optional
            Filter by variable name
        start_date : Union[str, datetime, pd.Timestamp], optional
            Start date for timeseries query.
            Accepts ISO 8601 string, datetime, or pd.Timestamp.
        end_date : Union[str, datetime, pd.Timestamp], optional
            End date for timeseries query.
            Accepts ISO 8601 string, datetime, or pd.Timestamp.
            If None, only start_date is used.
        load : bool, optional
            If True, load the downloaded data into the local evaluation
            "secondary_timeseries" table. Default: False
        write_mode : str, optional
            Write mode when loading. Options: "append", "upsert",
            "create_or_replace". Default: "append"
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        Union[pd.DataFrame, None]
            DataFrame with TEEHR secondary timeseries schema, or None if load=True

        Raises
        ------
        ValueError
            If neither primary_location_id nor secondary_location_id is provided

        Examples
        --------
        >>> # Fetch NWM retrospective for specific locations and date range
        >>> timeseries = ev.download.secondary_timeseries(
        ...     primary_location_id=["usgs-01010000", "usgs-01010500"],
        ...     configuration_name="nwm30_retrospective",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02"
        ... )
        >>> # Fetch and load into local evaluation
        >>> timeseries = ev.download.secondary_timeseries(
        ...     primary_location_id=["usgs-01010000"],
        ...     configuration_name="nwm30_retrospective",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02",
        ...     load=True
        ... )
        """
        if not primary_location_id and not secondary_location_id:
            raise ValueError(
                "Either primary_location_id or secondary_location_id must be provided"
            )

        params = {**kwargs}

        if primary_location_id:
            params["primary_location_id"] = primary_location_id
        if secondary_location_id:
            params["secondary_location_id"] = secondary_location_id
        if configuration_name:
            params["configuration_name"] = configuration_name
        if variable_name:
            params["variable_name"] = variable_name

        datetime_range = _format_datetime_range(start_date, end_date)
        if datetime_range:
            params["datetime"] = datetime_range

        response = self._make_request(
            "collections/secondary_timeseries/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        df = teehr_api_timeseries_to_dataframe(response.json())

        logger.info(f"Fetched {len(df)} secondary timeseries values from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="secondary_timeseries",
                write_mode=write_mode
            )
            return
        return df

    def evaluation_subset(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        primary_configuration_name: str,
        secondary_configuration_name: str,
        prefix: str = None,
        bbox: List[float] = None,
    ) -> None:
        """Download a subset of evaluation data based on location IDs, date range, and configurations.

        Parameters
        ----------
        start_date : Union[str, datetime, pd.Timestamp]
            Start date for timeseries query. Accepts ISO 8601 string, datetime, or pd.Timestamp.
        end_date : Union[str, datetime, pd.Timestamp]
            End date for timeseries query. Accepts ISO 8601 string, datetime, or pd.Timestamp.
        primary_configuration_name : str
            Name of the primary configuration to include.
        secondary_configuration_name : str
            Name of the secondary configuration to include.
        prefix : str, optional
            Filter locations by ID prefix (e.g., "usgs", "nwm30").
        bbox : list of float, optional
            Bounding box to filter locations by spatial extent,
            in the format [minx, miny, maxx, maxy].

        Returns
        -------
        None
            Loads the subset data to local iceberg tables.

        Examples
        --------
        >>> ev.download.evaluation_subset(
        ...     prefix="usgs",
        ...     bbox=[-120.0, 35.0, -119.0, 36.0],
        ...     start_date="2020-01-01",
        ...     end_date="2020-01-02",
        ...     primary_configuration_name="usgs_observations",
        ...     secondary_configuration_name="nwm30_retrospective"
        ... )
        """
        if prefix is None and bbox is None:
            raise ValueError(
                "At least one of prefix or bbox must be provided to filter locations"
            )
        logger.info("Loading the units, variables, and attributes tables")
        self.units(load=True)
        self.variables(load=True)
        self.attributes(load=True)
        logger.info("Loading the primary configuration name")
        self.configurations(
            name=primary_configuration_name,
            load=True
        )
        logger.info("Loading the secondary configuration name")
        self.configurations(
            name=secondary_configuration_name,
            load=True
        )
        logger.info("Loading the locations table")
        self.locations(
            prefix=prefix,
            include_attributes=False,
            bbox=bbox,
            load=True
        )
        if self._ev.locations.to_sdf().count() == 0:
            logger.warning("No locations found with the specified filters. "
                           "No timeseries or location attributes will be loaded.")
            return
        location_ids = self._ev.locations.to_sdf().select("id").rdd.flatMap(lambda x: x).collect()
        logger.info("Loading the primary timeseries data")
        self.primary_timeseries(
            primary_location_id=location_ids,
            configuration_name=primary_configuration_name,
            start_date=start_date,
            end_date=end_date,
            load=True
        )
        logger.info("Loading the location crosswalks")
        self.location_crosswalks(
            primary_location_id=location_ids,
            load=True
        )
        logger.info("Loading the secondary timeseries data")
        self.secondary_timeseries(
            primary_location_id=location_ids,
            configuration_name=secondary_configuration_name,
            start_date=start_date,
            end_date=end_date,
            load=True
        )
        logger.info("Loading the location attributes")
        self.location_attributes(
            location_id=location_ids,
            load=True
        )
        logger.info("Finished loading evaluation subset")
