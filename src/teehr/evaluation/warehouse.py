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


class Warehouse:
    """Component class for fetching data from the TEEHR warehouse."""

    def __init__(self, ev, api_base_url: str = None, verify_ssl: bool = False) -> None:
        """Initialize the Warehouse class.

        Parameters
        ----------
        ev : Evaluation
            The parent Evaluation instance providing access to tables,
            Spark session, and cache directories.
        api_base_url : str, optional
            Base URL for the TEEHR warehouse API.
            Default: "https://api.teehr.local.app.garden"
        verify_ssl : bool, optional
            Whether to verify SSL certificates when making requests.
            Default: False (for self-signed certificates)
        """
        # Now we have access to the Evaluation object.
        self._ev = ev
        self._load = ev.load
        self.api_base_url = api_base_url or "https://api.teehr.local.app.garden"
        self.verify_ssl = verify_ssl

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
        """
        url = f"{api_base_url}/{endpoint}"

        logger.debug(f"Making request to {url} with params {params}")

        response = requests.get(url, params=params or {}, verify=verify_ssl)
        response.raise_for_status()

        return response

    def get_locations(
        self,
        prefix: str = None,
        include_attributes: bool = False,
        limit: int = 10000,
        **kwargs
    ) -> gpd.GeoDataFrame:
        """Fetch locations from the warehouse API as a GeoDataFrame.

        Parameters
        ----------
        prefix : str, optional
            Filter locations by ID prefix (e.g., "usgs", "nwm30")
        include_attributes : bool, optional
            Whether to include location attributes in the response.
            Default: False
        limit : int, optional
            Maximum number of locations to return. Default: 10000
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame containing locations with geometry

        Examples
        --------
        >>> # Fetch USGS locations with attributes
        >>> locations = ev.warehouse.get_locations(
        ...     prefix="usgs",
        ...     include_attributes=True
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

        response = self._make_request(
            "collections/locations/items",
            self.api_base_url,
            self.verify_ssl,
            params
        )
        gdf = gpd.read_file(BytesIO(response.content))

        logger.info(f"Fetched {len(gdf)} locations from warehouse API")

        return gdf

    def get_attributes(
        self,
        name: str = None,
        type: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch attributes from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by attribute name
        type : str, optional
            Filter by attribute type ("categorical" or "continuous")
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing attribute definitions

        Examples
        --------
        >>> # Fetch all categorical attributes
        >>> attrs = ev.warehouse.get_attributes(type="categorical")
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

        return df

    def get_location_attributes(
        self,
        location_id: Union[str, List[str]] = None,
        attribute_name: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch location attributes from the warehouse API.

        Parameters
        ----------
        location_id : str or list of str, optional
            Filter by location ID(s)
        attribute_name : str, optional
            Filter by attribute name
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing location attribute values

        Examples
        --------
        >>> # Fetch attributes for specific locations
        >>> loc_ids = ["usgs-01010000", "usgs-01010500"]
        >>> loc_attrs = ev.warehouse.get_location_attributes(
        ...     location_id=loc_ids
        ... )
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

        return df

    def get_units(
        self,
        name: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch units from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by unit name
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing unit definitions

        Examples
        --------
        >>> # Fetch all units
        >>> units = ev.warehouse.get_units()
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

        return df

    def get_variables(
        self,
        name: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch variables from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by variable name
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing variable definitions

        Examples
        --------
        >>> # Fetch all variables
        >>> variables = ev.warehouse.get_variables()
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

        return df

    def get_configurations(
        self,
        name: str = None,
        type: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch configurations from the warehouse API.

        Parameters
        ----------
        name : str, optional
            Filter by configuration name
        type : str, optional
            Filter by configuration type ("primary" or "secondary")
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing configuration definitions

        Examples
        --------
        >>> # Fetch all primary configurations
        >>> configs = ev.warehouse.get_configurations(type="primary")
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

        return df

    def get_location_crosswalks(
        self,
        primary_location_id: Union[str, List[str]] = None,
        secondary_location_id: Union[str, List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch location crosswalks from the warehouse API.

        Parameters
        ----------
        primary_location_id : str or list of str, optional
            Filter by primary location ID(s)
        secondary_location_id : str or list of str, optional
            Filter by secondary location ID(s)
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame containing location crosswalk mappings

        Examples
        --------
        >>> # Fetch crosswalks for specific primary locations
        >>> loc_ids = ["usgs-01010000", "usgs-01010500"]
        >>> crosswalks = ev.warehouse.get_location_crosswalks(
        ...     primary_location_id=loc_ids
        ... )
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

        return df

    def get_primary_timeseries(
        self,
        primary_location_id: Union[str, List[str]],
        configuration_name: Union[str, List[str]],
        variable_name: str = None,
        start_date: Union[str, datetime, pd.Timestamp] = None,
        end_date: Union[str, datetime, pd.Timestamp] = None,
        **kwargs
    ) -> pd.DataFrame:
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
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame with TEEHR primary timeseries schema

        Examples
        --------
        >>> # Fetch USGS observations for specific locations and date range
        >>> timeseries = ev.warehouse.get_primary_timeseries(
        ...     primary_location_id=["usgs-01010000", "usgs-01010500"],
        ...     configuration_name="usgs_observations",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02"
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

        return df

    def get_secondary_timeseries(
        self,
        primary_location_id: Union[str, List[str]] = None,
        secondary_location_id: Union[str, List[str]] = None,
        configuration_name: Union[str, List[str]] = None,
        variable_name: str = None,
        start_date: Union[str, datetime, pd.Timestamp] = None,
        end_date: Union[str, datetime, pd.Timestamp] = None,
        **kwargs
    ) -> pd.DataFrame:
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
        **kwargs
            Additional query parameters to pass to the API

        Returns
        -------
        pd.DataFrame
            DataFrame with TEEHR secondary timeseries schema

        Raises
        ------
        ValueError
            If neither primary_location_id nor secondary_location_id is provided

        Examples
        --------
        >>> # Fetch NWM retrospective for specific locations and date range
        >>> timeseries = ev.warehouse.get_secondary_timeseries(
        ...     primary_location_id=["usgs-01010000", "usgs-01010500"],
        ...     configuration_name="nwm30_retrospective",
        ...     start_date="1990-10-01",
        ...     end_date="1990-10-02"
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

        return df