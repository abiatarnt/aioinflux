import asyncio
import json
import logging
import warnings
from typing import Union, AnyStr, Iterable, Optional, AsyncGenerator, Callable, Awaitable

import aiohttp

from . import serialization
from .client import runner, PointType, ResultType, InfluxDBError, InfluxDBWriteError
from .compat import *

# Aioinflux uses logging mainly for debugging purposes.
# Please attach your own handlers if you need logging.
logger = logging.getLogger('aioinflux')


class DefaultResponseHandler:
    """A Helper class to facilitate response handling
    """
    async def __call__(self, resp: aiohttp.ClientResponse) -> bytes:
        """Process response"""
        payload = await resp.read()
        return payload


class JsonHandler:
    def __init__(self, ok=200, parser=json.loads):
        self.parser = parser
        self.ok = ok

    async def __call__(self, resp):
        payload = await resp.read()
        retval = self.parser(payload)
        if resp.status != self.ok:
            raise RuntimeError(retval, resp.status)
        return retval


class InfluxDB2Client:
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 8086,
        path: str = '/',
        mode: str = 'async',
        output: str = 'csv',
        bucket: Optional[str] = None,
        org: Optional[str] = None,
        ssl: bool = True,
        *,
        unix_socket: Optional[str] = None,
        token: Optional[str] = None,
        timeout: Optional[Union[aiohttp.ClientTimeout, float]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs
    ):
        """
        :class:`~aioinflux.client.InfluxDBClient`  holds information necessary
        to interact with InfluxDB.
        It is async by default, but can also be used as a sync/blocking client.
        When querying, responses are returned as parsed JSON by default,
        but can also be wrapped in easily iterable
        wrapper object or be parsed to Pandas DataFrames.
        The three main public methods are the three endpoints of the InfluxDB API, namely:

        1. :meth:`~.InfluxDBClient.ping`
        2. :meth:`~.InfluxDBClient.write`
        3. :meth:`~.InfluxDBClient.query`

        See each of the above methods documentation for further usage details.

        See also: https://docs.influxdata.com/influxdb/latest/tools/api/

        :param host: Hostname to connect to InfluxDB.
        :param port: Port to connect to InfluxDB.
        :param path: Path to connect to InfluxDB.
        :param mode: Mode in which client should run. Available options:

           - ``async``: Default mode. Each query/request to the backend will
           - ``blocking``: Behaves in sync/blocking fashion,
             similar to the official InfluxDB-Python client.

        :param output: Output format of the response received from InfluxDB.

           - ``json``: Default format.
             Returns parsed JSON as received from InfluxDB.
           - ``dataframe``: Parses results into :py:class`pandas.DataFrame`.
             Not compatible with chunked responses.

        :param bucket: Default bucket name to be used by the client.
        :param org: Default organisation name to be used by the client.
        :param ssl: If https should be used.
        :param unix_socket: Path to the InfluxDB Unix domain socket.
        :param token: Authentication token used to connect to InfluxDB.
        :param timeout: Timeout in seconds or :class:`aiohttp.ClientTimeout` object
        :param loop: Asyncio event loop.
        :param kwargs: Additional kwargs for :class:`aiohttp.ClientSession`
        """
        self._loop = loop or asyncio.get_event_loop()
        self._session: aiohttp.ClientSession = None
        self._mode = None
        self._output = None
        self._bucket = None
        self.ssl = ssl
        self.host = host
        self.port = port
        self.path = path
        self.mode = mode
        self.output = output
        self.bucket = bucket
        self.org = org

        # ClientSession configuration
        if unix_socket:
            kwargs.update(connector=aiohttp.UnixConnector(unix_socket, loop=self._loop))
        if timeout:
            if isinstance(timeout, aiohttp.ClientTimeout):
                kwargs.update(timeout=timeout)
            else:
                kwargs.update(timeout=aiohttp.ClientTimeout(total=timeout))

        header = kwargs.setdefault('headers', {})
        header.update(Authorization=" ".join(["Token", token]))
        self.opts = kwargs

    async def create_session(self, **kwargs):
        """Creates an :class:`aiohttp.ClientSession`

        Override this or call it with ``kwargs`` to use other :mod:`aiohttp`
        functionality not covered by :class:`~.InfluxDBClient.__init__`
        """
        self.opts.update(kwargs)
        self._session = aiohttp.ClientSession(**self.opts, loop=self._loop)

    @property
    def url(self):
        protocol = "https" if self.ssl else "http"
        return f"{protocol}://{self.host}:{self.port}{self.path}{{endpoint}}"

    @property
    def mode(self):
        return self._mode

    @property
    def output(self):
        return self._output

    @property
    def bucket(self):
        return self._bucket

    @mode.setter
    def mode(self, mode):
        if mode not in ('async', 'blocking'):
            raise ValueError('Invalid running mode')
        self._mode = mode

    @output.setter
    def output(self, output):
        if pd is None and output == 'dataframe':
            raise ValueError(no_pandas_warning)
        if output not in ('csv', 'dataframe'):
            raise ValueError('Invalid output format')
        self._output = output

    @bucket.setter
    def bucket(self, bucket):
        self._bucket = bucket
        if not bucket:
            warnings.warn('No default bucket set. '
                          'Bucket must be specified when querying/writing.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __del__(self):
        if not self._loop.is_closed() and self._session:
            asyncio.ensure_future(self._session.close(), loop=self._loop)

    def __repr__(self):
        items = [f'{k}={v}' for k, v in vars(self).items() if not k.startswith('_')]
        items.append(f'mode={self.mode}')
        return f'{type(self).__name__}({", ".join(items)})'

    @runner
    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    @runner
    async def req(self,
                  method: str,
                  endpoint: str,
                  headers: Optional[dict] = None,
                  params: Optional[dict] = None,
                  data: Optional[dict] = None,
                  handler: Optional[Callable[[aiohttp.ClientResponse], Awaitable]]
                  = DefaultResponseHandler()):
        if not self._session:
            await self.create_session()
        url = self.url.format(endpoint=endpoint)
        async with self._session.request(method,
                                         url,
                                         headers=headers,
                                         params=params,
                                         data=data) as resp:
            logger.debug(f'{resp.status}: {resp.reason}')
            if handler is not None:
                content = await handler(resp)
            else:
                content = None
            return resp, content

    @runner
    async def ping(self) -> dict:
        """Pings InfluxDB

        Returns a dictionary containing the headers of the response from ``influxd``.
        """
        resp, payload = await self.req("get", "health", handler=None)
        return dict(resp.headers.items())

    @runner
    async def write(
        self,
        data: Union[PointType, Iterable[PointType]],
        measurement: Optional[str] = None,
        bucket: Optional[str] = None,
        org: Optional[str] = None,
        precision: Optional[str] = None,
        tag_columns: Optional[Iterable] = None,
        **extra_tags,
    ) -> bool:
        """Writes data to InfluxDB.
        Input can be:

        1. A mapping (e.g. ``dict``) containing the keys:
           ``measurement``, ``time``, ``tags``, ``fields``
        2. A Pandas :class:`~pandas.DataFrame` with a :class:`~pandas.DatetimeIndex`
        3. A user defined class decorated w/
            :func:`~aioinflux.serialization.usertype.lineprotocol`
        4. A string (``str`` or ``bytes``) properly formatted in InfluxDB's line protocol
        5. An iterable of one of the above

        Input data in formats 1-3 are parsed to the line protocol before being
        written to InfluxDB.
        See the `InfluxDB docs <https://docs.influxdata.com/influxdb/latest/
        write_protocols/line_protocol_reference/>`_ for more details.

        :param data: Input data (see description above).
        :param measurement: Measurement name. Mandatory when when writing DataFrames only.
            When writing dictionary-like data, this field is treated as the default value
            for points that do not contain a `measurement` field.
        :param db: Database to be written to. Defaults to `self.db`.
        :param precision: Sets the precision for the supplied Unix time values.
            Ignored if input timestamp data is of non-integer type.
            Valid values: ``{'ns', 'u', 'µ', 'ms', 's', 'm', 'h'}``
        :param rp: Sets the target retention policy for the write.
            If unspecified, data is written to the default retention policy.
        :param tag_columns: Columns to be treated as tags
            (used when writing DataFrames only)
        :param extra_tags: Additional tags to be added to all points passed.
            Valid when writing DataFrames or mappings only.
            Silently ignored for user-defined classes and raw lineprotocol
        :return: Returns ``True`` if insert is successful.
            Raises :py:class:`ValueError` otherwise.
        """
        headers = dict(Accept="application/json")
        params = {
            'bucket': bucket or self.bucket,
            'org': org or self.org
        }
        if precision is not None:
            if precision not in {"ms", "s", "us", "ns"}:
                raise ValueError("Invalid precision:", precision)
            params['precision'] = precision
        data = serialization.serialize(data, measurement, tag_columns, **extra_tags)
        resp, payload = await self.req("post",
                                       "api/v2/write",
                                       headers=headers,
                                       params=params,
                                       data=data,
                                       handler=None)
        if resp.status != 204:
            raise InfluxDBWriteError(resp)
        return True

    @runner
    async def query(
        self,
        q: AnyStr,
        *,
        bucket: Optional[str] = None,
        org: Optional[str] = None,
    ) -> Union[AsyncGenerator[ResultType, None], ResultType]:
        """Sends a query to InfluxDB.
        Please refer to the InfluxDB documentation for all the possible queries:
        https://docs.influxdata.com/influxdb/latest/query_language/

        :param q: Raw query string
        :param bucket: Optional bucket name. If specified, it overrides the
           default bucket name used by the client.
        :param org: Optional organisation name. If specified, it overrides the
           default organisation name used by the client.
        :return: Response in the format specified by the combination of
           :attr:`.InfluxDBClient.output` and ``chunked``
        """
        data = dict(query=q, bucket=bucket or self.bucket, type="influxql")
        resp, payload = await self.req(
            "post",
            "query",
            headers={"Content-type": "application/json",
                     "Accept": "application/csv"},
            params={"org": org or self.org},
            data=data)

        if resp.status != 200:
            data = json.loads(data)
            self._check_error(data)
            return data

        if self.output == 'csv':
            return data
        elif self.output == 'dataframe':
            return serialization.dataframe.parse(data)
        else:
            raise ValueError('Invalid output format')

    @runner
    async def flux_query(
        self,
        q: AnyStr,
        *,
        dialect: Optional[dict] = None,
        extern: Optional[dict] = None,
        org: Optional[str] = None,
    ) -> Union[AsyncGenerator[ResultType, None], ResultType]:
        """Sends a query to InfluxDB.
        Please refer to the InfluxDB documentation for all the possible queries:
        https://docs.influxdata.com/influxdb/latest/query_language/

        :param q: Raw query string
        :param dialect: Options specifying the csv dialect used for the result.
        :param extern: Options specifying an external file.
        :param org: Optional organisation name. If specified, it overrides the
           default organisation name used by the client.

        :return: Response in the format specified by the combination of
           :attr:`.InfluxDBClient.output` and ``chunked``
        """
        data = dict(query=q, type="flux")
        if dialect:
            data['dialect'] = dialect

        if extern:
            data['extern'] = extern
        resp, payload = await self.req(
            "post",
            "api/v2/query",
            headers={"Content-type": "application/json",
                     "Accept": "application/csv"},
            params={"org": org or self.org},
            data=data)
        logger.debug(f'{resp.status}: {q}')

        data = json.loads(data)
        self._check_error(data)
        if self.output == 'json':
            return data
        elif self.output == 'dataframe':
            return serialization.dataframe.parse(data)
        else:
            raise ValueError('Invalid output format')

    @staticmethod
    def _check_error(response):
        """Checks for JSON error messages and raises Python exception"""
        if 'error' in response:
            raise InfluxDBError(response['error'])
        elif 'results' in response:
            for statement in response['results']:
                if 'error' in statement:
                    msg = '{d[error]} (statement {d[statement_id]})'
                    raise InfluxDBError(msg.format(d=statement))

    @runner
    async def list_buckets(self, **kwargs):
        resp, payload = await self.req("get",
                                       "api/v2/buckets",
                                       params=kwargs,
                                       handler=JsonHandler())
        return payload['buckets']

    @runner
    async def list_orgs(self, **kwargs):
        resp, payload = await self.req("get",
                                       "api/v2/orgs",
                                       params=kwargs,
                                       handler=JsonHandler())
        return payload['orgs']

    @runner
    async def list_users(self):
        resp, payload = await self.req(
            "get",
            "api/v2/users",
            handler=JsonHandler())
        return payload['users']
