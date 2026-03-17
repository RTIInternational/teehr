"""Component class for fetching data from from the TEEHR data warehouse."""
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
    """A component class for downloading data from the TEEHR-Cloud data warehouse."""

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
        ids: Union[str, List[str]] = None,
        bbox: List[float] = None,
        include_attributes: bool = False,
        page_size: int = 10000,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[gpd.GeoDataFrame, None]:
        """Fetch locations from the warehouse API as a GeoDataFrame.

        Parameters
        ----------
        prefix : str, optional
            Filter locations by ID prefix (e.g., "usgs", "nwm30")
        ids : str or list of str, optional
            Filter locations by specific IDs.
        bbox : list of float, optional
            Bounding box to filter locations by spatial extent,
            in the format [minx, miny, maxx, maxy].
        include_attributes : bool, optional
            Whether to include location attributes in the response.
            Default: False
        page_size : int, optional
            Number of locations to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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
        params = {**kwargs}

        if prefix:
            params["prefix"] = prefix
        if include_attributes:
            params["include_attributes"] = "true"
        if bbox:
            params["bbox"] = ",".join(map(str, bbox))
        if ids:
            params["id"] = ids

        all_gdfs = []
        page_params = {**params, 'limit': page_size}
        current_offset = 0

        while True:
            page_params['offset'] = current_offset
            response = self._make_request(
                "collections/locations/items",
                self.api_base_url,
                self.verify_ssl,
                page_params
            )
            gdf = gpd.read_file(BytesIO(response.content))

            all_gdfs.append(gdf)
            logger.debug(
                f"Fetched page offset={current_offset} with {len(gdf)} locations"
            )

            if len(gdf) < page_size:
                break

            current_offset += page_size

        if not all_gdfs:
            return gpd.GeoDataFrame()
        gdf = pd.concat(all_gdfs, ignore_index=True) if len(all_gdfs) > 1 else all_gdfs[0]

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
        page_size: int = 10000,
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
        page_size : int, optional
            Number of attributes to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_items(
            "collections/attributes/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

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
        page_size: int = 10000,
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
        page_size : int, optional
            Number of location attributes to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_items(
            "collections/location_attributes/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

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
        page_size: int = 10000,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch units from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by unit name
        page_size : int, optional
            Number of units to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_items(
            "collections/units/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

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
        page_size: int = 10000,
        load: bool = False,
        write_mode: str = "append",
        **kwargs
    ) -> Union[pd.DataFrame, None]:
        """Fetch variables from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by variable name
        page_size : int, optional
            Number of variables to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_items(
            "collections/variables/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

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
        page_size: int = 10000,
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
        page_size : int, optional
            Number of configurations to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_items(
            "collections/configurations/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

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
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        page_size: int = 10000,
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
        primary_location_id_prefix : str, optional
            Filter crosswalks by primary location ID prefix (e.g., "usgs").
            Passed as a query parameter to the API. Default: None
        secondary_location_id_prefix : str, optional
            Filter crosswalks by secondary location ID prefix (e.g., "nwm30").
            Passed as a query parameter to the API. Default: None
        page_size : int, optional
            Number of location crosswalks to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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
        >>> # Fetch crosswalks filtered by ID prefixes
        >>> crosswalks = ev.download.location_crosswalks(
        ...     primary_location_id_prefix="usgs",
        ...     secondary_location_id_prefix="nwm30"
        ... )
        >>> # Fetch and load into local evaluation
        >>> crosswalks = ev.download.location_crosswalks(load=True)
        """
        params = {**kwargs}

        if primary_location_id:
            params["primary_location_id"] = primary_location_id
        if secondary_location_id:
            params["secondary_location_id"] = secondary_location_id
        if primary_location_id_prefix:
            params["primary_location_id_prefix"] = primary_location_id_prefix
        if secondary_location_id_prefix:
            params["secondary_location_id_prefix"] = secondary_location_id_prefix

        items = self._fetch_paginated_items(
            "collections/location_crosswalks/items",
            params,
            page_size=page_size
        )
        df = pd.DataFrame(items)

        logger.info(f"Fetched {len(df)} location crosswalks from warehouse API")
        if load:
            self._load.dataframe(
                df=df,
                table_name="location_crosswalks",
                write_mode=write_mode
            )
            return
        return df

    def _fetch_paginated_items(
        self,
        endpoint: str,
        params: dict,
        page_size: int,
    ) -> list:
        """Fetch all pages from a JSON items endpoint using limit/offset pagination.

        Parameters
        ----------
        endpoint : str
            API endpoint path (e.g., "collections/attributes/items")
        params : dict
            Base query parameters (without limit/offset)
        page_size : int
            Number of items to request per page

        Returns
        -------
        list
            All items accumulated across all pages
        """
        all_items = []
        page_params = {**params, 'limit': page_size}
        current_offset = 0

        while True:
            page_params['offset'] = current_offset
            response = self._make_request(
                endpoint,
                self.api_base_url,
                self.verify_ssl,
                page_params
            )
            page_items = response.json()["items"]

            all_items.extend(page_items)
            logger.debug(
                f"Fetched page offset={current_offset} with {len(page_items)} items from {endpoint}"
            )

            if len(page_items) < page_size:
                break

            current_offset += page_size

        return all_items

    def _fetch_paginated_timeseries(
        self,
        endpoint: str,
        params: dict,
        page_size: int,
    ) -> list:
        """Fetch all pages from a timeseries endpoint using limit/offset pagination.

        Parameters
        ----------
        endpoint : str
            API endpoint path (e.g., "collections/primary_timeseries/items")
        params : dict
            Base query parameters (without limit/offset)
        page_size : int
            Number of series items to request per page

        Returns
        -------
        list
            All series items accumulated across all pages
        """
        all_items = []
        page_params = {**params, 'limit': page_size}
        current_offset = 0

        while True:
            page_params['offset'] = current_offset
            response = self._make_request(
                endpoint,
                self.api_base_url,
                self.verify_ssl,
                page_params
            )
            page_data = response.json()

            # Normalise to list — API may return a single dict or a list
            page_items = [page_data] if isinstance(page_data, dict) else page_data

            all_items.extend(page_items)
            logger.debug(
                f"Fetched page offset={current_offset} with {len(page_items)} items from {endpoint}"
            )

            if len(page_items) < page_size:
                break

            current_offset += page_size

        return all_items

    def primary_timeseries(
        self,
        primary_location_id: Union[str, List[str]],
        configuration_name: Union[str, List[str]],
        variable_name: str = None,
        start_date: Union[str, datetime, pd.Timestamp] = None,
        end_date: Union[str, datetime, pd.Timestamp] = None,
        load: bool = False,
        write_mode: str = "append",
        page_size: int = 10000,
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
        page_size : int, optional
            Number of series items to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_timeseries(
            "collections/primary_timeseries/items",
            params,
            page_size=page_size,
        )
        df = teehr_api_timeseries_to_dataframe(items)

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
        page_size: int = 10000,
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
        page_size : int, optional
            Number of series items to fetch per API request.
            Decrease if timeout errors are encountered. Default: 10000.
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

        items = self._fetch_paginated_timeseries(
            "collections/secondary_timeseries/items",
            params,
            page_size=page_size,
        )
        df = teehr_api_timeseries_to_dataframe(items)

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
        location_ids: Union[str, List[str]] = None,
        prefix: str = None,
        bbox: List[float] = None,
        page_size: int = 10000,
    ) -> None:
        """Download a subset of evaluation data from the warehouse API.

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
        location_ids : str or list of str, optional
            Location ID or list of location IDs to include in the subset.
        prefix : str, optional
            Filter locations by ID prefix (e.g., "usgs", "nwm30").
        bbox : list of float, optional
            Bounding box to filter locations by spatial extent,
            in the format [minx, miny, maxx, maxy].
        page_size : int, optional
            Number of series items to fetch per API request for timeseries.
            Decrease if timeout errors are encountered. Default: 10000

        Returns
        -------
        None
            Loads the subset data to local iceberg tables.

        Examples
        --------
        >>> ev.download.evaluation_subset(
        ...     prefix="usgs",
        ...     bbox=[-120.0, 35.0, -119.0, 36.0],
        ...     start_date="2005-01-01",
        ...     end_date="2020-01-02",
        ...     primary_configuration_name="usgs_observations",
        ...     secondary_configuration_name="nwm30_retrospective",
        ...     page_size=5000
        ... )
        """
        if prefix is None and bbox is None and location_ids is None:
            raise ValueError(
                "At least one of prefix, bbox, or location_ids must be provided to filter locations"
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
            ids=location_ids,
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
            load=True,
            page_size=page_size
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
            load=True,
            page_size=page_size
        )
        logger.info("Loading the location attributes")
        self.location_attributes(
            location_id=location_ids,
            load=True
        )
        logger.info("Finished loading evaluation subset")
