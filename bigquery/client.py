import calendar
import json
from logging import getLogger
from collections import defaultdict
from datetime import datetime, timedelta
from hashlib import sha256
from io import StringIO
from time import sleep, time

import six
from bigquery.errors import (BigQueryTimeoutException, JobExecutingException,
                             JobInsertException, UnfinishedQueryException)
from googleapiclient.discovery import build, DISCOVERY_URI
from googleapiclient.errors import HttpError
from httplib2 import Http

BIGQUERY_SCOPE = [
    'https://www.googleapis.com/auth/bigquery'
]

BIGQUERY_SCOPE_READ_ONLY = [
    'https://www.googleapis.com/auth/bigquery.readonly'
]

CACHE_TIMEOUT = timedelta(seconds=30)

JOB_CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
JOB_CREATE_NEVER = 'CREATE_NEVER'
JOB_WRITE_TRUNCATE = 'WRITE_TRUNCATE'
JOB_WRITE_APPEND = 'WRITE_APPEND'
JOB_WRITE_EMPTY = 'WRITE_EMPTY'
JOB_ENCODING_UTF_8 = 'UTF-8'
JOB_ENCODING_ISO_8859_1 = 'ISO-8859-1'
JOB_PRIORITY_INTERACTIVE = 'INTERACTIVE'
JOB_PRIORITY_BATCH = 'BATCH'
JOB_COMPRESSION_NONE = 'NONE'
JOB_COMPRESSION_GZIP = 'GZIP'

JOB_FORMAT_CSV = 'CSV'
JOB_FORMAT_NEWLINE_DELIMITED_JSON = 'NEWLINE_DELIMITED_JSON'
JOB_SOURCE_FORMAT_DATASTORE_BACKUP = 'DATASTORE_BACKUP'
JOB_SOURCE_FORMAT_NEWLINE_DELIMITED_JSON = JOB_FORMAT_NEWLINE_DELIMITED_JSON
JOB_SOURCE_FORMAT_CSV = JOB_FORMAT_CSV
JOB_DESTINATION_FORMAT_AVRO = 'AVRO'
JOB_DESTINATION_FORMAT_NEWLINE_DELIMITED_JSON = \
    JOB_FORMAT_NEWLINE_DELIMITED_JSON
JOB_DESTINATION_FORMAT_CSV = JOB_FORMAT_CSV

logger = getLogger(__name__)


def get_client(project_id=None, credentials=None,
               service_url=None, service_account=None,
               private_key=None, private_key_file=None,
               json_key=None, json_key_file=None,
               readonly=True, swallow_results=True):
    """Return a singleton instance of BigQueryClient. Either
    AssertionCredentials or a service account and private key combination need
    to be provided in order to authenticate requests to BigQuery.

    Parameters
    ----------
    project_id : str, optional
        The BigQuery project id, required unless json_key or json_key_file is
        provided.
    credentials : oauth2client.client.SignedJwtAssertionCredentials, optional
        AssertionCredentials instance to authenticate requests to BigQuery
        (optional, must provide `service_account` and (`private_key` or
        `private_key_file`) or (`json_key` or `json_key_file`) if not included
    service_url : str, optional
        A URI string template pointing to the location of Google's API
        discovery service. Requires two parameters {api} and {apiVersion} that
        when filled in produce an absolute URI to the discovery document for
        that service. If not set then the default googleapiclient discovery URI
        is used. See `credentials`
    service_account : str, optional
        The Google API service account name. See `credentials`
    private_key : str, optional
        The private key associated with the service account in PKCS12 or PEM
        format. See `credentials`
    private_key_file : str, optional
        The name of the file containing the private key associated with the
        service account in PKCS12 or PEM format. See `credentials`
    json_key : dict, optional
        The JSON key associated with the service account. See `credentials`
    json_key_file : str, optional
        The name of the JSON key file associated with the service account. See
        `credentials`.
    readonly : bool
        Bool indicating if BigQuery access is read-only. Has no effect if
        credentials are provided. Default True.
    swallow_results : bool
        If set to False, then return the actual response value instead of
        converting to boolean. Default True.

    Returns
    -------
    BigQueryClient
        An instance of the BigQuery client.
    """

    if not credentials:
        assert (service_account and (private_key or private_key_file)) or (
            json_key or json_key_file), \
            'Must provide AssertionCredentials or service account and P12 key\
            or JSON key'

    if not project_id:
        assert json_key or json_key_file, \
            'Must provide project_id unless json_key or json_key_file is\
            provided'

    if service_url is None:
        service_url = DISCOVERY_URI

    scope = BIGQUERY_SCOPE_READ_ONLY if readonly else BIGQUERY_SCOPE

    if private_key_file:
        credentials = _credentials().from_p12_keyfile(service_account,
                                                      private_key_file,
                                                      scopes=scope)

    if private_key:
        try:
            if isinstance(private_key, basestring):
                private_key = private_key.decode('utf-8')
        except NameError:
            # python3 -- private_key is already unicode
            pass
        credentials = _credentials().from_p12_keyfile_buffer(
            service_account,
            StringIO(private_key),
            scopes=scope)

    if json_key_file:
        with open(json_key_file, 'r') as key_file:
            json_key = json.load(key_file)

    if json_key:
        credentials = _credentials().from_json_keyfile_dict(json_key,
                                                            scopes=scope)
        if not project_id:
            project_id = json_key['project_id']

    bq_service = _get_bq_service(credentials=credentials,
                                 service_url=service_url)

    return BigQueryClient(bq_service, project_id, swallow_results)


def _get_bq_service(credentials=None, service_url=None):
    """Construct an authorized BigQuery service object."""

    assert credentials, 'Must provide ServiceAccountCredentials'

    http = credentials.authorize(Http())
    service = build('bigquery', 'v2', http=http,
                    discoveryServiceUrl=service_url)

    return service


def _credentials():
    """Import and return SignedJwtAssertionCredentials class"""
    from oauth2client.service_account import ServiceAccountCredentials

    return ServiceAccountCredentials


class BigQueryClient(object):

    def __init__(self, bq_service, project_id, swallow_results=True):
        self.bigquery = bq_service
        self.project_id = project_id
        self.swallow_results = swallow_results
        self.cache = {}

    def _submit_query_job(self, query_data):
        """ Submit a query job to BigQuery.

            This is similar to BigQueryClient.query, but gives the user
            direct access to the query method on the offical BigQuery
            python client.

            For fine-grained control over a query job, see:
            https://google-api-client-libraries.appspot.com/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html#query

        Parameters
        ----------
        query_data
            query object as per "configuration.query" in
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query

        Returns
        -------
        tuple
            job id and query results if query completed. If dry_run is True,
            job id will be None and results will be empty if the query is valid
            or a dict containing the response if invalid.

        Raises
        ------
        BigQueryTimeoutException
            On timeout
        """

        logger.debug('Submitting query job: %s' % query_data)

        job_collection = self.bigquery.jobs()

        try:
            query_reply = job_collection.query(
                projectId=self.project_id, body=query_data).execute()
        except HttpError as e:
            if query_data.get("dryRun", False):
                return None, json.loads(e.content.decode('utf8'))
            raise

        job_id = query_reply['jobReference'].get('jobId')
        schema = query_reply.get('schema', {'fields': None})['fields']
        rows = query_reply.get('rows', [])
        job_complete = query_reply.get('jobComplete', False)

        # raise exceptions if it's not an async query
        # and job is not completed after timeout
        if not job_complete and query_data.get("timeoutMs", False):
            logger.error('BigQuery job %s timeout' % job_id)
            raise BigQueryTimeoutException()

        return job_id, [self._transform_row(row, schema) for row in rows]

    def _insert_job(self, body_object):
        """ Submit a job to BigQuery

            Direct proxy to the insert() method of the offical BigQuery
            python client.

            Able to submit load, link, query, copy, or extract jobs.

            For more details, see:
            https://google-api-client-libraries.appspot.com/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html#insert

        Parameters
        ----------
        body_object : body object passed to bigquery.jobs().insert()

        Returns
        -------
        response of the bigquery.jobs().insert().execute() call

        Raises
        ------
        BigQueryTimeoutException on timeout
        """

        logger.debug('Submitting job: %s' % body_object)

        job_collection = self.bigquery.jobs()

        return job_collection.insert(
            projectId=self.project_id,
            body=body_object
        ).execute()

    def query(self, query, max_results=None, timeout=0, dry_run=False):
        """Submit a query to BigQuery.

        Parameters
        ----------
        query : str
            BigQuery query string
        max_results : int, optional
            The maximum number of rows to return per page of results.
        timeout : float, optional
            How long to wait for the query to complete, in seconds before
            the request times out and returns.
        dry_run : bool, optional
            If True, the query isn't actually run. A valid query will return an
            empty response, while an invalid one will return the same error
            message it would if it wasn't a dry run.

        Returns
        -------
        tuple
            (job id, query results) if the query completed. If dry_run is True,
            job id will be None and results will be empty if the query is valid
            or a ``dict`` containing the response if invalid.

        Raises
        ------
        BigQueryTimeoutException
            on timeout
        """

        logger.debug('Executing query: %s' % query)

        query_data = {
            'query': query,
            'timeoutMs': timeout * 1000,
            'dryRun': dry_run,
            'maxResults': max_results,
        }
        return self._submit_query_job(query_data)

    def get_query_schema(self, job_id):
        """Retrieve the schema of a query by job id.

        Parameters
        ----------
        job_id : str
            The job_id that references a BigQuery query

        Returns
        -------
        list
            A ``list`` of ``dict`` objects that represent the schema.
        """

        query_reply = self.get_query_results(job_id, offset=0, limit=0)

        if not query_reply['jobComplete']:
            logger.warning('BigQuery job %s not complete' % job_id)
            raise UnfinishedQueryException()

        return query_reply['schema']['fields']

    def get_table_schema(self, dataset, table):
        """Return the table schema.

        Parameters
        ----------
        dataset : str
            The dataset containing the `table`.
        table : str
            The table to get the schema for

        Returns
        -------
        list
            A ``list`` of ``dict`` objects that represent the table schema. If
            the table doesn't exist, None is returned.
        """

        try:
            result = self.bigquery.tables().get(
                projectId=self.project_id,
                tableId=table,
                datasetId=dataset).execute()
        except HttpError as e:
            if int(e.resp['status']) == 404:
                logger.warn('Table %s.%s does not exist', dataset, table)
                return None
            raise

        return result['schema']['fields']

    def check_job(self, job_id):
        """Return the state and number of results of a query by job id.

        Parameters
        ----------
        job_id : str
            The job id of the query to check.

        Returns
        -------
        tuple
            (``bool``, ``int``) Whether or not the query has completed and the
            total number of rows included in the query table if it has
            completed (else 0)
        """

        query_reply = self.get_query_results(job_id, offset=0, limit=0)

        return (query_reply.get('jobComplete', False),
                int(query_reply.get('totalRows', 0)))

    def get_query_rows(self, job_id, offset=None, limit=None, timeout=0):
        """Retrieve a list of rows from a query table by job id.
        This method will append results from multiple pages together. If you
        want to manually page through results, you can use `get_query_results`
        method directly.

        Parameters
        ----------
        job_id : str
            The job id that references a BigQuery query.
        offset : int, optional
            The offset of the rows to pull from BigQuery
        limit : int, optional
            The number of rows to retrieve from a query table.
        timeout : float, optional
            Timeout in seconds.

        Returns
        -------
        list
            A ``list`` of ``dict`` objects that represent table rows.
        """

        # Get query results
        query_reply = self.get_query_results(job_id, offset=offset,
                                             limit=limit, timeout=timeout)
        if not query_reply['jobComplete']:
            logger.warning('BigQuery job %s not complete' % job_id)
            raise UnfinishedQueryException()

        schema = query_reply["schema"]["fields"]
        rows = query_reply.get('rows', [])
        page_token = query_reply.get("pageToken")
        records = [self._transform_row(row, schema) for row in rows]

        # Append to records if there are multiple pages for query results
        while page_token and (not limit or len(records) < limit):
            query_reply = self.get_query_results(
                job_id, offset=offset, limit=limit, page_token=page_token,
                timeout=timeout)
            page_token = query_reply.get("pageToken")
            rows = query_reply.get('rows', [])
            records += [self._transform_row(row, schema) for row in rows]
        return records[:limit] if limit else records

    def check_dataset(self, dataset_id):
        """Check to see if a dataset exists.

        Parameters
        ----------
        dataset_id : str
            Dataset unique id

        Returns
        -------
        bool
            True if dataset at `dataset_id` exists, else Fasle
        """
        dataset = self.get_dataset(dataset_id)
        return bool(dataset)

    def get_dataset(self, dataset_id):
        """Retrieve a dataset if it exists, otherwise return an empty dict.

        Parameters
        ----------
        dataset_id : str
            Dataset unique id

        Returns
        -------
        dict
            Contains dataset object if it exists, else empty
        """
        try:
            dataset = self.bigquery.datasets().get(
                projectId=self.project_id, datasetId=dataset_id).execute()
        except HttpError:
            dataset = {}

        return dataset

    def check_table(self, dataset, table):
        """Check to see if a table exists.

        Parameters
        ----------
        dataset : str
            The dataset to check
        table : str
            The name of the table

        Returns
        -------
        bool
            True if table exists, else False
        """
        table = self.get_table(dataset, table)
        return bool(table)

    def get_table(self, dataset, table):
        """ Retrieve a table if it exists, otherwise return an empty dict.

        Parameters
        ----------
        dataset : str
            The dataset that the table is in
        table : str
            The name of the table

        Returns
        -------
        dict
            Containing the table object if it exists, else empty
        """
        try:
            table = self.bigquery.tables().get(
                projectId=self.project_id, datasetId=dataset,
                tableId=table).execute()
        except HttpError:
            table = {}

        return table

    def create_table(self, dataset, table, schema, expiration_time=None):
        """Create a new table in the dataset.

        Parameters
        ----------
        dataset : str
            The dataset to create the table in
        table : str
            The name of the table to create
        schema : dict
            The table schema
        expiration_time : float, optional
            The expiry time in milliseconds since the epoch.

        Returns
        -------
        Union[bool, dict]
            If the table was successfully created, or response from BigQuery
            if swallow_results is set to False
        """

        body = {
            'schema': {'fields': schema},
            'tableReference': {
                'tableId': table,
                'projectId': self.project_id,
                'datasetId': dataset
            }
        }

        if expiration_time is not None:
            body['expirationTime'] = expiration_time

        try:
            table = self.bigquery.tables().insert(
                projectId=self.project_id,
                datasetId=dataset,
                body=body
            ).execute()
            if self.swallow_results:
                return True
            else:
                return table

        except HttpError as e:
            logger.error(('Cannot create table {0}.{1}\n'
                          'Http Error: {2}').format(dataset, table, e.content))
            if self.swallow_results:
                return False
            else:
                return {}

    def update_table(self, dataset, table, schema):
        """Update an existing table in the dataset.

        Parameters
        ----------
        dataset : str
            The dataset to update the table in
        table : str
            The name of the table to update
        schema : dict
            Table schema

        Returns
        -------
        Union[bool, dict]
            bool indicating if the table was successfully updated or not,
            or response from BigQuery if swallow_results is set to False.
        """

        body = {
            'schema': {'fields': schema},
            'tableReference': {
                'tableId': table,
                'projectId': self.project_id,
                'datasetId': dataset
            }
        }

        try:
            result = self.bigquery.tables().update(
                projectId=self.project_id,
                datasetId=dataset,
                body=body
            ).execute()
            if self.swallow_results:
                return True
            else:
                return result

        except HttpError as e:
            logger.error(('Cannot update table {0}.{1}\n'
                          'Http Error: {2}').format(dataset, table, e.content))
            if self.swallow_results:
                return False
            else:
                return {}

    def patch_table(self, dataset, table, schema):
        """Patch an existing table in the dataset.

        Parameters
        ----------
        dataset : str
            The dataset to patch the table in
        table : str
            The name of the table to patch
        schema : dict
            The table schema

        Returns
        -------
        Union[bool, dict]
            Bool indicating if the table was successfully patched or not,
            or response from BigQuery if swallow_results is set to False
        """

        body = {
            'schema': {'fields': schema},
            'tableReference': {
                'tableId': table,
                'projectId': self.project_id,
                'datasetId': dataset
            }
        }

        try:
            result = self.bigquery.tables().patch(
                projectId=self.project_id,
                datasetId=dataset,
                body=body
            ).execute()
            if self.swallow_results:
                return True
            else:
                return result

        except HttpError as e:
            logger.error(('Cannot patch table {0}.{1}\n'
                          'Http Error: {2}').format(dataset, table, e.content))
            if self.swallow_results:
                return False
            else:
                return {}

    def create_view(self, dataset, view, query):
        """Create a new view in the dataset.

        Parameters
        ----------
        dataset : str
            The dataset to create the view in
        view : str
            The name of the view to create
        query : dict
            A query that BigQuery executes when the view is referenced.

        Returns
        -------
        Union[bool, dict]
            bool indicating if the view was successfully created or not,
            or response from BigQuery if swallow_results is set to False.
        """

        body = {
            'tableReference': {
                'tableId': view,
                'projectId': self.project_id,
                'datasetId': dataset
            },
            'view': {
                'query': query
            }
        }

        try:
            view = self.bigquery.tables().insert(
                projectId=self.project_id,
                datasetId=dataset,
                body=body
            ).execute()
            if self.swallow_results:
                return True
            else:
                return view

        except HttpError as e:
            logger.error(('Cannot create view {0}.{1}\n'
                          'Http Error: {2}').format(dataset, view, e.content))
            if self.swallow_results:
                return False
            else:
                return {}

    def delete_table(self, dataset, table):
        """Delete a table from the dataset.

        Parameters
        ----------
        dataset : str
            The dataset to delete the table from.
        table : str
            The name of the table to delete

        Returns
        -------
        Union[bool, dict]
            bool indicating if the table was successfully deleted or not,
            or response from BigQuery if swallow_results is set for False.
        """

        try:
            response = self.bigquery.tables().delete(
                projectId=self.project_id,
                datasetId=dataset,
                tableId=table
            ).execute()
            if self.swallow_results:
                return True
            else:
                return response

        except HttpError as e:
            logger.error(('Cannot delete table {0}.{1}\n'
                          'Http Error: {2}').format(dataset, table, e.content))
            if self.swallow_results:
                return False
            else:
                return {}

    def get_tables(self, dataset_id, app_id, start_time, end_time):
        """Retrieve a list of tables that are related to the given app id
        and are inside the range of start and end times.

        Parameters
        ----------
        dataset_id : str
            The BigQuery dataset id to consider.
        app_id : str
            The appspot name
        start_time : Union[datetime, int]
            The datetime or unix time after which records will be fetched.
        end_time : Union[datetime, int]
            The datetime or unix time up to which records will be fetched.

        Returns
        -------
        list
            A ``list`` of table names.
        """

        if isinstance(start_time, datetime):
            start_time = calendar.timegm(start_time.utctimetuple())

        if isinstance(end_time, datetime):
            end_time = calendar.timegm(end_time.utctimetuple())

        every_table = self._get_all_tables(dataset_id)
        app_tables = every_table.get(app_id, {})

        return self._filter_tables_by_time(app_tables, start_time, end_time)

    def import_data_from_uris(
            self,
            source_uris,
            dataset,
            table,
            schema=None,
            job=None,
            source_format=None,
            create_disposition=None,
            write_disposition=None,
            encoding=None,
            ignore_unknown_values=None,
            max_bad_records=None,
            allow_jagged_rows=None,
            allow_quoted_newlines=None,
            field_delimiter=None,
            quote=None,
            skip_leading_rows=None,
    ):
        """
        Imports data into a BigQuery table from cloud storage. Optional
        arguments that are not specified are determined by BigQuery as
        described:
            https://developers.google.com/bigquery/docs/reference/v2/jobs

        Parameters
        ----------
        source_urls : list
            A ``list`` of ``str`` objects representing the urls on cloud
            storage of the form: gs://bucket/filename
        dataset : str
            String id of the dataset
        table : str
            String id of the table
        job : str, optional
            Identifies the job (a unique job id is automatically generated if
            not provided)
        schema : list, optional
            Represents the BigQuery schema
        source_format : str, optional
            One of the JOB_SOURCE_FORMAT_* constants
        create_disposition : str, optional
            One of the JOB_CREATE_* constants
        write_disposition : str, optional
            One of the JOB_WRITE_* constants
        encoding : str, optional
            One of the JOB_ENCODING_* constants
        ignore_unknown_values : bool, optional
            Whether or not to ignore unknown values
        max_bad_records : int, optional
            Maximum number of bad records
        allow_jagged_rows : bool, optional
            For csv only
        allow_quoted_newlines : bool, optional
            For csv only
        field_delimiter : str, optional
            For csv only
        quote : str, optional
            Quote character for csv only
        skip_leading_rows : int, optional
            For csv only

        Returns
        -------
        dict
            A BigQuery job response

        Raises
        ------
        JobInsertException
            on http/auth failures or error in result
        """
        source_uris = source_uris if isinstance(source_uris, list) \
            else [source_uris]

        configuration = {
            "destinationTable": {
                "projectId": self.project_id,
                "tableId": table,
                "datasetId": dataset
            },
            "sourceUris": source_uris,
        }

        if max_bad_records:
            configuration['maxBadRecords'] = max_bad_records

        if ignore_unknown_values:
            configuration['ignoreUnknownValues'] = ignore_unknown_values

        if create_disposition:
            configuration['createDisposition'] = create_disposition

        if write_disposition:
            configuration['writeDisposition'] = write_disposition

        if encoding:
            configuration['encoding'] = encoding

        if schema:
            configuration['schema'] = {'fields': schema}

        if source_format:
            configuration['sourceFormat'] = source_format

        if not job:
            hex = self._generate_hex_for_uris(source_uris)
            job = "{dataset}-{table}-{digest}".format(
                dataset=dataset,
                table=table,
                digest=hex
            )

        if source_format == JOB_SOURCE_FORMAT_CSV:
            if field_delimiter:
                configuration['fieldDelimiter'] = field_delimiter

            if allow_jagged_rows:
                configuration['allowJaggedRows'] = allow_jagged_rows

            if allow_quoted_newlines:
                configuration['allowQuotedNewlines'] = allow_quoted_newlines

            if quote:
                configuration['quote'] = quote

            if skip_leading_rows:
                configuration['skipLeadingRows'] = skip_leading_rows

        elif field_delimiter or allow_jagged_rows \
                or allow_quoted_newlines or quote or skip_leading_rows:
            all_values = dict(field_delimiter=field_delimiter,
                              allow_jagged_rows=allow_jagged_rows,
                              allow_quoted_newlines=allow_quoted_newlines,
                              skip_leading_rows=skip_leading_rows,
                              quote=quote)
            non_null_values = dict((k, v) for k, v
                                   in list(all_values.items())
                                   if v)
            raise Exception("Parameters field_delimiter, allow_jagged_rows, "
                            "allow_quoted_newlines, quote and "
                            "skip_leading_rows are only allowed when "
                            "source_format=JOB_SOURCE_FORMAT_CSV: %s"
                            % non_null_values)

        body = {
            "configuration": {
                'load': configuration
            },
            "jobReference": {
                "projectId": self.project_id,
                "jobId": job
            }
        }

        logger.debug("Creating load job %s" % body)
        job_resource = self._insert_job(body)
        self._raise_insert_exception_if_error(job_resource)
        return job_resource

    def export_data_to_uris(
            self,
            destination_uris,
            dataset,
            table,
            job=None,
            compression=None,
            destination_format=None,
            print_header=None,
            field_delimiter=None,
    ):
        """
        Export data from a BigQuery table to cloud storage. Optional arguments
        that are not specified are determined by BigQuery as described:
        https://developers.google.com/bigquery/docs/reference/v2/jobs

        Parameters
        ----------
        destination_urls : Union[str, list]
            ``str`` or ``list`` of ``str`` objects representing the URIs on
            cloud storage of the form: gs://bucket/filename
        dataset : str
            String id of the dataset
        table : str
            String id of the table
        job : str, optional
            String identifying the job (a unique jobid is automatically
            generated if not provided)
        compression : str, optional
            One of the JOB_COMPRESSION_* constants
        destination_format : str, optional
            One of the JOB_DESTination_FORMAT_* constants
        print_header : bool, optional
            Whether or not to print the header
        field_delimiter : str, optional
            Character separating fields in delimited file

        Returns
        -------
        dict
            A BigQuery job resource

        Raises
        ------
        JobInsertException
            On http/auth failures or error in result
        """
        destination_uris = destination_uris \
            if isinstance(destination_uris, list) else [destination_uris]

        configuration = {
            "sourceTable": {
                "projectId": self.project_id,
                "tableId": table,
                "datasetId": dataset
            },
            "destinationUris": destination_uris,
        }

        if compression:
            configuration['compression'] = compression

        if destination_format:
            configuration['destinationFormat'] = destination_format

        if print_header is not None:
            configuration['printHeader'] = print_header

        if field_delimiter:
            configuration['fieldDelimiter'] = field_delimiter

        if not job:
            hex = self._generate_hex_for_uris(destination_uris)
            job = "{dataset}-{table}-{digest}".format(
                dataset=dataset,
                table=table,
                digest=hex
            )

        body = {
            "configuration": {
                'extract': configuration
            },
            "jobReference": {
                "projectId": self.project_id,
                "jobId": job
            }
        }

        logger.info("Creating export job %s" % body)
        job_resource = self._insert_job(body)
        self._raise_insert_exception_if_error(job_resource)
        return job_resource

    def write_to_table(
            self,
            query,
            dataset=None,
            table=None,
            external_udf_uris=[],
            allow_large_results=None,
            use_query_cache=None,
            priority=None,
            create_disposition=None,
            write_disposition=None,
    ):
        """
        Write query result to table. If dataset or table is not provided,
        Bigquery will write the result to temporary table. Optional arguments
        that are not specified are determined by BigQuery as described:
        https://developers.google.com/bigquery/docs/reference/v2/jobs

        Parameters
        ----------
        query : str
            BigQuery query string
        dataset : str, optional
            String id of the dataset
        table : str, optional
            String id of the table
        external_udf_uris : list, optional
            Contains extternal UDF URIs. If given, URIs must be Google Cloud
            Storage and have .js extensions.
        allow_large_results : bool, optional
            Whether or not to allow large results
        use_query_cache : bool, optional
            Whether or not to use query cache
        priority : str, optional
            One of the JOB_PRIORITY_* constants
        create_disposition : str, optional
            One of the JOB_CREATE_* constants
        write_disposition : str, optional
            One of the JOB_WRITE_* constants

        Returns
        -------
        dict
            A BigQuery job resource

        Raises
        ------
        JobInsertException
            On http/auth failures or error in result
        """

        configuration = {
            "query": query,
        }

        if dataset and table:
            configuration['destinationTable'] = {
                "projectId": self.project_id,
                "tableId": table,
                "datasetId": dataset
            }

        if allow_large_results is not None:
            configuration['allowLargeResults'] = allow_large_results

        if use_query_cache is not None:
            configuration['useQueryCache'] = use_query_cache

        if priority:
            configuration['priority'] = priority

        if create_disposition:
            configuration['createDisposition'] = create_disposition

        if write_disposition:
            configuration['writeDisposition'] = write_disposition

        configuration['userDefinedFunctionResources'] = []
        for external_udf_uri in external_udf_uris:
            configuration['userDefinedFunctionResources'].append(
                {
                    "resourceUri": external_udf_uri
                }
            )

        body = {
            "configuration": {
                'query': configuration
            }
        }

        logger.info("Creating write to table job %s" % body)
        job_resource = self._insert_job(body)
        self._raise_insert_exception_if_error(job_resource)
        return job_resource

    def wait_for_job(self, job, interval=5, timeout=60):
        """
        Waits until the job indicated by job_resource is done or has failed

        Parameters
        ----------
        job : Union[dict, str]
            ``dict`` representing a BigQuery job resource, or a ``str``
            representing the BigQuery job id
        interval : float, optional
            Polling interval in seconds, default = 5
        timeout : float, optional
            Timeout in seconds, default = 60

        Returns
        -------
        dict
            Final state of the job resouce, as described here:
            https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html#get

        Raises
        ------
        Union[JobExecutingException, BigQueryTimeoutException]
            On http/auth failures or timeout
        """
        complete = False
        job_id = str(job if isinstance(job,
                                       (six.binary_type, six.text_type, int))
                     else job['jobReference']['jobId'])
        job_resource = None

        start_time = time()
        elapsed_time = 0
        while not (complete or elapsed_time > timeout):
            sleep(interval)
            request = self.bigquery.jobs().get(projectId=self.project_id,
                                               jobId=job_id)
            job_resource = request.execute()
            self._raise_executing_exception_if_error(job_resource)
            complete = job_resource.get('status').get('state') == u'DONE'
            elapsed_time = time() - start_time

        # raise exceptions if timeout
        if not complete:
            logger.error('BigQuery job %s timeout' % job_id)
            raise BigQueryTimeoutException()

        return job_resource

    def push_rows(self, dataset, table, rows, insert_id_key=None,
                  skip_invalid_rows=None, ignore_unknown_values=None,
                  template_suffix=None):
        """Upload rows to BigQuery table.

        Parameters
        ----------
        dataset : str
            The dataset to upload to
        table : str
            The name of the table to insert rows into
        rows : list
            A ``list`` of rows (``dict`` objects) to add to the table
        insert_id_key : str, optional
            Key for insertId in row
        skip_invalid_rows : bool, optional
            Insert all valid rows of a request, even if invalid rows exist.
        ignore_unknown_values : bool, optional
            Accept rows that contain values that do not match the schema.
        template_suffix : str, optional
            Inserts the rows into an {table}{template_suffix}.
            If table {table}{template_suffix} doesn't exist, create from {table}.

        Returns
        -------
        Union[bool, dict]
            bool indicating if insert succeeded or not, or response
            from BigQuery if swallow_results is set for False.
        """

        table_data = self.bigquery.tabledata()

        rows_data = []
        for row in rows:
            each_row = {}
            each_row["json"] = row
            if insert_id_key in row:
                each_row["insertId"] = row[insert_id_key]
            rows_data.append(each_row)

        data = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": rows_data
        }

        if skip_invalid_rows is not None:
            data['skipInvalidRows'] = skip_invalid_rows

        if ignore_unknown_values is not None:
            data['ignoreUnknownValues'] = ignore_unknown_values

        if template_suffix is not None:
            data['templateSuffix'] = template_suffix

        try:
            response = table_data.insertAll(
                projectId=self.project_id,
                datasetId=dataset,
                tableId=table,
                body=data
            ).execute()

            if response.get('insertErrors'):
                logger.error('BigQuery insert errors: %s' % response)
                if self.swallow_results:
                    return False
                else:
                    return response

            if self.swallow_results:
                return True
            else:
                return response

        except HttpError as e:
            logger.exception('Problem with BigQuery insertAll')
            if self.swallow_results:
                return False
            else:
                return {
                    'insertErrors': [{
                        'errors': [{
                            'reason': 'httperror',
                            'message': e
                        }]
                    }]
                }

    def get_all_tables(self, dataset_id):
        """Retrieve a list of tables for the dataset.

        Parameters
        ----------
        dataset_id : str
            The dataset to retrieve table data for.

        Returns
        -------
        A ``list`` with all table names
        """
        tables_data = self._get_all_tables_for_dataset(dataset_id)

        tables = []
        for table in tables_data.get('tables', []):
            table_name = table.get('tableReference', {}).get('tableId')
            if table_name:
                tables.append(table_name)
        return tables

    def _get_all_tables(self, dataset_id, cache=False):
        """Retrieve the list of tables for dataset, that respect the formats:
            * appid_YYYY_MM
            * YYYY_MM_appid

        Parameters
        ----------
        dataset_id : str
            The dataset to retrieve table names for
        cache : bool, optional
            To use cached value or not (default False). Timeout value equals
            CACHE_TIMEOUT.

        Returns
        -------
        dict
            A ``dict`` of app ids mapped to their table names
        """
        do_fetch = True
        if cache and self.cache.get(dataset_id):
            time, result = self.cache.get(dataset_id)
            if datetime.now() - time < CACHE_TIMEOUT:
                do_fetch = False

        if do_fetch:
            result = self._get_all_tables_for_dataset(dataset_id)
            self.cache[dataset_id] = (datetime.now(), result)

        return self._parse_table_list_response(result)

    def _get_all_tables_for_dataset(self, dataset_id):
        """Retrieve a list of all tables for the dataset.

        Parameters
        ----------
        dataset_id : str
            The dataset to retrieve table names for

        Returns
        -------
        dict
            A ``dict`` containing tables key with all tables
        """
        result = self.bigquery.tables().list(
            projectId=self.project_id,
            datasetId=dataset_id).execute()

        page_token = result.get('nextPageToken')
        while page_token:
            res = self.bigquery.tables().list(
                projectId=self.project_id,
                datasetId=dataset_id,
                pageToken=page_token
            ).execute()
            page_token = res.get('nextPageToken')
            result['tables'] += res.get('tables', [])
        return result

    def _parse_table_list_response(self, list_response):
        """Parse the response received from calling list on tables.

        Parameters
        ----------
        list_response
            The response found by calling list on a BigQuery table object.

        Returns
        -------
        dict
            Dates referenced by table names
        """

        tables = defaultdict(dict)

        for table in list_response.get('tables', []):
            table_ref = table.get('tableReference')

            if not table_ref:
                continue

            table_id = table_ref.get('tableId', '')

            year_month, app_id = self._parse_table_name(table_id)

            if not year_month:
                continue

            table_date = datetime.strptime(year_month, '%Y-%m')
            unix_seconds = calendar.timegm(table_date.timetuple())
            tables[app_id].update({table_id: unix_seconds})

        # Turn off defualting
        tables.default_factory = None

        return tables

    def _parse_table_name(self, table_id):
        """Parse a table name in the form of appid_YYYY_MM or
        YYYY_MM_appid and return a tuple consisting of YYYY-MM and the app id.

        Parameters
        ----------
        table_id : str
            The table id as listed by BigQuery

        Returns
        -------
        tuple
            (year/month, app id), or (None, None) if the table id cannot be
            parsed.
        """

        # Prefix date
        attributes = table_id.split('_')
        year_month = "-".join(attributes[:2])
        app_id = "-".join(attributes[2:])

        # Check if date parsed correctly
        if year_month.count("-") == 1 and all(
                [num.isdigit() for num in year_month.split('-')]):
            return year_month, app_id

        # Postfix date
        attributes = table_id.split('_')
        year_month = "-".join(attributes[-2:])
        app_id = "-".join(attributes[:-2])

        # Check if date parsed correctly
        if year_month.count("-") == 1 and all(
                [num.isdigit() for num in year_month.split('-')]):
            return year_month, app_id

        return None, None

    def _filter_tables_by_time(self, tables, start_time, end_time):
        """Filter a table dictionary and return table names based on the range
        of start and end times in unix seconds.

        Parameters
        ----------
        tables : dict
            Dates referenced by table names
        start_time : int
            The unix time after which records will be fetched
        end_time : int
            The unix time up to which records will be fetched

        Returns
        -------
        list
            Table names that are inside the time range
        """

        return [table_name for (table_name, unix_seconds) in tables.items()
                if self._in_range(start_time, end_time, unix_seconds)]

    def _in_range(self, start_time, end_time, time):
        """Indicate if the given time falls inside of the given range.

        Parameters
        ----------
        start_time : int
            The unix time for the start of the range
        end_time : int
            The unix time for the end of the range
        time : int
            The unix time to check

        Returns
        -------
        bool
            True if the time falls within the range, False otherwise.
        """

        ONE_MONTH = 2764800  # 32 days

        return start_time <= time <= end_time or \
            time <= start_time <= time + ONE_MONTH or \
            time <= end_time <= time + ONE_MONTH

    def get_query_results(self, job_id, offset=None, limit=None,
                          page_token=None, timeout=0):
        """Execute the query job indicated by the given job id. This is direct
        mapping to bigquery api
        https://cloud.google.com/bigquery/docs/reference/v2/jobs/getQueryResults

        Parameters
        ----------
        job_id : str
            The job id of the query to check
        offset : optional
            The index the result set should start at.
        limit : int, optional
            The maximum number of results to retrieve.
        page_token : optional
            Page token, returned by previous call, to request the next page of
            results.
        timeout : float, optional
            Timeout in seconds

        Returns
        -------
        out
            The query reply
        """

        job_collection = self.bigquery.jobs()
        return job_collection.getQueryResults(
            projectId=self.project_id,
            jobId=job_id,
            startIndex=offset,
            maxResults=limit,
            pageToken=page_token,
            timeoutMs=timeout * 1000).execute()

    def _transform_row(self, row, schema):
        """Apply the given schema to the given BigQuery data row.

        Parameters
        ----------
        row
            A single BigQuery row to transform
        schema : list
            The BigQuery table schema to apply to the row, specifically
            the list of field dicts.

        Returns
        -------
        dict
            Mapping schema to row
        """

        log = {}

        # Match each schema column with its associated row value
        for index, col_dict in enumerate(schema):
            col_name = col_dict['name']
            row_value = row['f'][index]['v']

            if row_value is None:
                log[col_name] = None
                continue

            # Recurse on nested records
            if col_dict['type'] == 'RECORD':
                row_value = self._recurse_on_row(col_dict, row_value)

            # Otherwise just cast the value
            elif col_dict['type'] == 'INTEGER':
                row_value = int(row_value)

            elif col_dict['type'] == 'FLOAT':
                row_value = float(row_value)

            elif col_dict['type'] == 'BOOLEAN':
                row_value = row_value in ('True', 'true', 'TRUE')

            elif col_dict['type'] == 'TIMESTAMP':
                row_value = float(row_value)

            log[col_name] = row_value

        return log

    def _recurse_on_row(self, col_dict, nested_value):
        """Apply the schema specified by the given dict to the nested value by
        recursing on it.

        Parameters
        ----------
        col_dict : dict
            The schema to apply to the nested value.
        nested_value : A value nested in a BigQuery row.

        Returns
        -------
        Union[dict, list]
            ``dict`` or ``list`` of ``dict`` objects from applied schema.
        """

        row_value = None

        # Multiple nested records
        if col_dict['mode'] == 'REPEATED' and isinstance(nested_value, list):
            row_value = [self._transform_row(record['v'], col_dict['fields'])
                         for record in nested_value]

        # A single nested record
        else:
            row_value = self._transform_row(nested_value, col_dict['fields'])

        return row_value

    def _generate_hex_for_uris(self, uris):
        """Given uris, generate and return hex version of it

        Parameters
        ----------
        uris : list
            Containing all uris

        Returns
        -------
        str
            Hexed uris
        """
        return sha256((":".join(uris) + str(time())).encode()).hexdigest()

    def _raise_insert_exception_if_error(self, job):
        error_http = job.get('error')
        if error_http:
            raise JobInsertException(
                "Error in export job API request: {0}".format(error_http))
        # handle errorResult - API request is successful but error in result
        error_result = job.get('status').get('errorResult')
        if error_result:
            raise JobInsertException(
                "Reason:{reason}. Message:{message}".format(**error_result))

    def _raise_executing_exception_if_error(self, job):
        error_http = job.get('error')
        if error_http:
            raise JobExecutingException(
                "Error in export job API request: {0}".format(error_http))
        # handle errorResult - API request is successful but error in result
        error_result = job.get('status').get('errorResult')
        if error_result:
            raise JobExecutingException(
                "Reason:{reason}. Message:{message}".format(**error_result))

    #
    # DataSet manipulation methods
    #
    def create_dataset(self, dataset_id, friendly_name=None, description=None,
                       access=None):
        """Create a new BigQuery dataset.

        Parameters
        ----------
        dataset_id : str
            Unique ``str`` identifying the dataset with the project (the
            referenceID of the dataset, not the integer id of the dataset)
        friendly_name: str, optional
            A human readable name
        description: str, optional
            Longer string providing a description
        access : list, optional
            Indicating access permissions (see
            https://developers.google.com/bigquery/docs/reference/v2/datasets#resource)

        Returns
        -------
        Union[bool, dict]
            ``bool`` indicating if dataset was created or not, or response
            from BigQuery if swallow_results is set for False
        """
        try:
            datasets = self.bigquery.datasets()
            dataset_data = self.dataset_resource(dataset_id,
                                                 friendly_name=friendly_name,
                                                 description=description,
                                                 access=access)

            response = datasets.insert(projectId=self.project_id,
                                       body=dataset_data).execute()
            if self.swallow_results:
                return True
            else:
                return response
        except HttpError as e:
            logger.error(
                'Cannot create dataset {0}, {1}'.format(dataset_id, e))
            if self.swallow_results:
                return False
            else:
                return {}

    def get_datasets(self):
        """List all datasets in the project.

        Returns
        -------
        list
            Dataset resources
        """
        try:
            datasets = self.bigquery.datasets()
            request = datasets.list(projectId=self.project_id)
            result = request.execute()
            return result.get('datasets', [])
        except HttpError as e:
            logger.error("Cannot list datasets: {0}".format(e))
            return None

    def delete_dataset(self, dataset_id, delete_contents=False):
        """Delete a BigQuery dataset.

        Parameters
        ----------
        dataset_id : str
            Unique ``str`` identifying the datset with the project (the
            referenceId of the dataset)
        delete_contents : bool, optional
            If True, forces the deletion of the dataset even when the dataset
            contains data (Default = False)

        Returns
        -------
        Union[bool, dict[
            ool indicating if the delete was successful or not, or response
            from BigQuery if swallow_results is set for False

        Raises
        -------
        HttpError
            404 when dataset with dataset_id does not exist
        """
        try:
            datasets = self.bigquery.datasets()
            request = datasets.delete(projectId=self.project_id,
                                      datasetId=dataset_id,
                                      deleteContents=delete_contents)
            response = request.execute()
            if self.swallow_results:
                return True
            else:
                return response
        except HttpError as e:
            logger.error(
                'Cannot delete dataset {0}: {1}'.format(dataset_id, e))
            if self.swallow_results:
                return False
            else:
                return {}

    def update_dataset(self, dataset_id, friendly_name=None, description=None,
                       access=None):
        """Updates information in an existing dataset. The update method
        replaces the entire dataset resource, whereas the patch method only
        replaces fields that are provided in the submitted dataset resource.

        Parameters
        ----------
        dataset_id : str
            Unique ``str`` identifying the dataset with the project (the
            referencedId of the dataset)
        friendly_name : str, optional
            An optional descriptive name for the dataset.
        description : str, optional
            An optional description of the dataset.
        access : list, optional
            Indicating access permissions

        Returns
        -------
        Union[bool, dict]
            ``bool`` indicating if the update was successful or not, or
            response from BigQuery if swallow_results is set for False.
        """
        try:
            datasets = self.bigquery.datasets()
            body = self.dataset_resource(dataset_id, friendly_name,
                                         description, access)
            request = datasets.update(projectId=self.project_id,
                                      datasetId=dataset_id,
                                      body=body)
            response = request.execute()
            if self.swallow_results:
                return True
            else:
                return response
        except HttpError as e:
            logger.error(
                'Cannot update dataset {0}: {1}'.format(dataset_id, e))
            if self.swallow_results:
                return False
            else:
                return {}

    def patch_dataset(self, dataset_id, friendly_name=None, description=None,
                      access=None):
        """Updates information in an existing dataset. The update method
        replaces the entire dataset resource, whereas the patch method only
        replaces fields that are provided in the submitted dataset resource.

        Parameters
        ----------
        dataset_id : str
            Unique string idenfitying the dataset with the project (the
            referenceId of the dataset)
        friendly_name : str, optional
            An optional descriptive name for the dataset.
        description : str, optional
            An optional description of the dataset.
        access : list, optional
            Indicating access permissions.

        Returns
        -------
        Union[bool, dict]
            ``bool`` indicating if the patch was successful or not, or response
            from BigQuery if swallow_results is set for False.
        """
        try:
            datasets = self.bigquery.datasets()
            body = self.dataset_resource(dataset_id, friendly_name,
                                         description, access)
            request = datasets.patch(projectId=self.project_id,
                                     datasetId=dataset_id, body=body)
            response = request.execute()
            if self.swallow_results:
                return True
            else:
                return response
        except HttpError as e:
            logger.error('Cannot patch dataset {0}: {1}'.format(dataset_id, e))
            if self.swallow_results:
                return False
            else:
                return {}

    def dataset_resource(self, ref_id, friendly_name=None, description=None,
                         access=None):
        """See
        https://developers.google.com/bigquery/docs/reference/v2/datasets#resource

        Parameters
        ----------
        ref_id : str
            Dataset id (the reference id, not the integer id)
        friendly_name : str, optional
            An optional descriptive name for the dataset
        description : str, optional
            An optional description for the dataset
        access : list, optional
            Indicating access permissions

        Returns
        -------
        dict
            Representing BigQuery dataset resource
        """
        data = {
            "datasetReference": {
                "datasetId": ref_id,
                "projectId": self.project_id
            }
        }
        if friendly_name:
            data["friendlyName"] = friendly_name
        if description:
            data["description"] = description
        if access:
            data["access"] = access

        return data

    @classmethod
    def schema_from_record(cls, record):
        """Given a dict representing a record instance to be inserted into
        BigQuery, calculate the schema.

        Parameters
        ----------
        record : dict
            representing a record to be inserted into big query,
            where all keys are ``str`` objects (representing column names in
            the record) and all values are of type ``int``, ``str``,
            ``unicode``, ``float``, ``bool``, ``datetime``, or ``dict``. A
            ``dict`` value represents a record, and must conform to the same
            restrictions as record.

        Returns
        -------
        list
            BigQuery schema

        Notes
        -----
        Results are undefined if a different value type is provided for a
        repeated field: E.g.

        >>> { rfield: [ { x: 1}, {x: "a string"} ] } # undefined!
        """
        from bigquery.schema_builder import schema_from_record
        return schema_from_record(record)
