from typing import Optional, Any, Dict, Generator, List, Union, Tuple, Literal
from datetime import datetime

import requests
from tqdm.auto import tqdm

from turbiner._logging import get_logger

APIResponseItem = Dict[str, Any]
APIResponse = List[APIResponseItem]

logger = get_logger(__name__)


def _populate_params(params: Dict[str, Any], param_name: str, value: Optional[Any] = None, ensure_list: bool = False):
    """
    In place populate request params dictionary with optional values.

    If parameter value is None, it is ignored and not added to the params dictionary.

    Args:
        params: Dictionary containing parameters.
        param_name: Name of the parameter.
        value: Optional value to include
        ensure_list: Whether to convert value to a list.
    """
    if value is None:
        return

    if ensure_list:
        value = value if isinstance(value, list) else [value]

    params[param_name] = value


class OpenAQ:
    """
    Wrapper for Open Air Quality API.
    """

    def __init__(
            self,
            server: str = 'https://api.openaq.org',
            api_version: str = 'v2',
            session: Optional[requests.Session] = None,
            page_size: int = 1000,
            pbar: bool = False,
    ):
        """
        Instantiate the OpenAQ wrapper.

        Args:
            server: URL of the server. Should not be changed normally.
            api_version: URL version.
            session: Optional session to use when making requests.
            page_size: Default page size for the paginated requests.
            pbar: Whether to show progressbar when downloading multi-page data.
        """
        self.page_size = page_size
        self.server = server.rstrip('/')
        self.api_version = api_version.strip('/')
        self.pbar = pbar
        self._req = session or requests

    def _make_url(self, endpoint: str) -> str:
        """Constructs endpoint URL from server URL, API version and endpoint name."""
        return f'{self.server}/{self.api_version}/{endpoint.strip("/")}'

    def _get_all_generator(self, endpoint: str, **kwargs) -> Generator[APIResponseItem, None, None]:
        """
        Return (lazily) all results from the API endpoint.

        Args:
            endpoint: API method to call.
            **kwargs: Arguments to the `requests.get` method. Page key of the params is ignored.

        Yields:
            One page of results returned from the server.
        """
        params = kwargs.pop('params', {})

        page = 1
        result_count = 0
        results_total: Optional[int] = None

        with tqdm(disable=not self.pbar) as pbar:
            while results_total is None or result_count < results_total:
                params['page'] = page
                response = self._get(endpoint, params=params, **kwargs)
                response.raise_for_status()

                response_json: Dict[str, Any] = response.json()
                results_total = response_json['meta']['found']

                if pbar.total is None:
                    pbar.total = results_total

                pbar.update(len(response_json['results']))

                yield from (item for item in response_json['results'])

                result_count += len(response_json['results'])

                if len(response_json['results']) == 0:
                    break

                page += 1

    def _get_all(
            self,
            endpoint: str,
            lazy: bool = True,
            **kwargs,
    ) -> Union[APIResponse, Generator[APIResponseItem, None, None]]:
        """
        Wrapper around the `OpenAQ._get_all_generator`. Does nothing when `lazy` is True, collects
         results from the `OpenAQ._get_all_generator` and returns all pages at once otherwise.

        Args:
            endpoint:  API method to call.
            lazy: Whether to return generator or results.
            **kwargs: Arguments to the `requests.get` method. Page key of the params is ignored.
        """
        generator = self._get_all_generator(endpoint, **kwargs)
        if lazy:
            return generator
        else:
            return list(generator)

    def _get(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Send GET request at the specified endpoint and return response.

        Args:
            endpoint: API method to call.
            **kwargs: Arguments to the `requests.get` method.

        Returns:
            Response from the server for the GET request to the `endpoint`.
        """
        headers = kwargs.pop('headers', {})
        headers.setdefault('Accept', 'application/json')

        params = kwargs.pop('params', {})
        params.setdefault('limit', self.page_size)

        return self._req.get(
            self._make_url(endpoint),
            headers=headers,
            params=params,
            **kwargs,
        )

    def countries(
            self,
            country: Optional[str] = None,
            lazy: bool = True,
            **kwargs,
    ) -> Union[APIResponse, Generator[APIResponseItem, None, None]]:
        """
        Countries API method.

        Args:
            country: Two character country-code.
            lazy: Whether to return generator over results or results.
            **kwargs: Arguments to the `requests.get` method.
        """
        params = kwargs.pop('params', {})
        if country is not None:
            params['country_id'] = country

        return self._get_all('countries', lazy=lazy, params=params, **kwargs)

    def cities(
            self,
            country: Optional[Union[str, List[str]]] = None,
            city: Optional[Union[str, List[str]]] = None,
            lazy: bool = True,
            **kwargs,
    ) -> Union[APIResponse, Generator[APIResponseItem, None, None]]:
        """
        Cities API method.

        Args:
            country: Single or multiple (list) countries to query.
            city: Single or multiple (list) cities to query.
            lazy: Whether to return generator over results or results.
            **kwargs: Arguments to the `requests.get` method.
        """
        params = kwargs.pop('params', {})
        if country is not None:
            params['country_id' if isinstance(country, str) else 'country'] = country
        if city is not None:
            params['city'] = city if isinstance(city, list) else [city]

        return self._get_all('cities', lazy=lazy, params=params, **kwargs)

    def locations(
            self,
            location: Optional[Union[int, List[str]]] = None,
            has_geo: Optional[bool] = None,
            parameter: Optional[Union[str, List[str]]] = None,
            coordinates: Optional[Union[str, Tuple[float, float]]] = None,
            radius: Optional[int] = None,
            country: Optional[Union[str, List[str]]] = None,
            city: Optional[Union[str, List[str]]] = None,
            is_mobile: Optional[bool] = None,
            is_analysis: Optional[bool] = None,
            source_name: Optional[Union[str, List[str]]] = None,
            entity: Optional[str] = None,
            sensor_type: Optional[str] = None,
            dump_raw: Optional[bool] = None,
            lazy: bool = True,
            **kwargs,
    ) -> Union[APIResponse, Generator[APIResponseItem, None, None]]:
        """
        Locations API method.

        Args:
            location: ID of the location.
            has_geo:
            parameter: A parameter name or ID by which to filter measurement results.
            coordinates: Coordinate pair in form lat,lng. Up to 8 decimal points of precision.
            radius: Search radius from coordinates as center in meters. Maximum of 100,000 (100km)
                defaults to 1000 (1km).
            country: Limit results by a certain country (or countries) using two letter country
                code (or a list of codes).
            city: Limit results by a certain city (or cities).
            is_mobile: Whether location is mobile.
            is_analysis: Whether data is the product of a previous analysis/aggregation and not raw measurements.
            source_name: Name of the data source.
            entity: Source entity type (one of: government, community, research).
            sensor_type: Type of Sensor (one of: reference grade, low-cost sensor).
            dump_raw:
            lazy: Whether to return generator over results or results.
            **kwargs: Arguments to the `requests.get` method.
        """
        params = kwargs.pop('params', {})

        _populate_params(params, 'location' if isinstance(location, list) else 'location_id', location)
        _populate_params(params, 'country', country, ensure_list=True)
        _populate_params(params, 'city', city, ensure_list=True)
        _populate_params(params, 'has_geo', has_geo)
        _populate_params(params, 'parameter', parameter, ensure_list=True)
        _populate_params(params, 'radius', radius)
        _populate_params(params, 'isMobile', is_mobile)
        _populate_params(params, 'isAnalysis', is_analysis)
        _populate_params(params, 'sourceName', source_name, ensure_list=True)
        _populate_params(params, 'entity', entity)
        _populate_params(params, 'sensorType', sensor_type)
        _populate_params(params, 'dumpRaw', dump_raw)

        if coordinates is not None:
            params[coordinates] = coordinates if isinstance(coordinates, str) else '{:.8f},{:.8f}'.format(*coordinates)

        return self._get_all('locations', lazy=lazy, params=params, **kwargs)

    def averages(
            self,
            date_from: Union[str, datetime],
            date_to: Union[str, datetime],
            parameter: Optional[Union[str, List[str]]] = None,
            country: Optional[Union[str, List[str]]] = None,
            city: Optional[Union[str, List[str]]] = None,
            location: Optional[Union[int, List[str]]] = None,
            spatial: Literal['location', 'country', 'total'] = 'location',
            temporal: Literal['hour', 'day', 'month', 'year'] = 'hour',
            lazy: bool = True,
            **kwargs,
    ) -> Union[APIResponse, Generator[APIResponseItem, None, None]]:
        """
        Averages API endpoint.

        Return measurements filtered by parameters and averaged over `spatial` and `temporal` dimensions.

        Args:
            date_from: Start of the date range.
            date_to: End of the date range.
            parameter: Name(s) of the parameter(s) to filter by.
            country: Country to filter by.
            city: City to filter by.
            location: Name or ID of the location to filter measurements by.
            spatial: Spatial averaging.
            temporal: Temporal averaging.
            lazy: Whether to return generator over results or results.
            **kwargs: Arguments to the `requests.get` method.
        """
        params = kwargs.pop('params', {})
        params['group'] = False

        _populate_params(params, 'date_from', date_from.isoformat() if isinstance(date_from, datetime) else date_from)
        _populate_params(params, 'date_to', date_to.isoformat() if isinstance(date_to, datetime) else date_to)
        _populate_params(params, 'location', location, ensure_list=True)
        _populate_params(params, 'country', country, ensure_list=True)
        _populate_params(params, 'city', city, ensure_list=True)
        _populate_params(params, 'parameter', parameter, ensure_list=True)
        _populate_params(params, 'spatial', spatial)
        _populate_params(params, 'temporal', temporal)

        return self._get_all('averages', lazy=lazy, params=params, **kwargs)
