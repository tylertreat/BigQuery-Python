from collections import defaultdict
from datetime import datetime
import logging
import time

from apiclient.discovery import build
import httplib2
from oauth2client.client import SignedJwtAssertionCredentials


BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery.readonly"


def get_client(project_id, credentials=None, service_account=None,
               private_key=None):
    """Return a singleton instance of BigQueryClient. Either
    AssertionCredentials or a service account and private key combination need
    to be provided in order to authenticate requests to BigQuery.

    Args:
        project_id: the BigQuery project id.
        credentials: an AssertionCredentials instance to authenticate requests
                     to BigQuery.
        service_account: the Google API service account name.
        private_key: the private key associated with the service account in
                     PKCS12 or PEM format.

    Returns:
        an instance of BigQueryClient.
    """

    if not credentials and not (service_account and private_key):
        raise Exception('AssertionCredentials or service account and private'
                        'key need to be provided')

    bq_service = _get_bq_service(credentials=credentials,
                                 service_account=service_account,
                                 private_key=private_key)

    return BigQueryClient(bq_service, project_id)


def _get_bq_service(credentials=None, service_account=None, private_key=None):
    """Construct an authorized BigQuery service object."""

    assert credentials or (service_account and private_key)

    if not credentials:
        credentials = SignedJwtAssertionCredentials(
            service_account, private_key, scope=BIGQUERY_SCOPE)

    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build('bigquery', 'v2', http=http)

    return service


class BigQueryClient(object):

    def __init__(self, bq_service, project_id):
        self.bigquery = bq_service
        self.project_id = project_id

    def query(self, query):
        """Submit a query to BigQuery.

        Args:
            query: BigQuery query string.

        Returns:
            a job id that acts as a pointer to the query results.
        """

        logging.debug('Executing query: %s' % query)

        job_collection = self.bigquery.jobs()
        query_data = {'query': query, 'timeoutMs': 0}

        query_reply = job_collection.query(
            projectId=self.project_id, body=query_data).execute()

        job_reference = query_reply['jobReference']

        return job_reference['jobId']

    def get_query_schema(self, job_id):
        """Retrieve the schema of a query by job id.

        Args:
            job_id: The job_id that references a BigQuery query.
        Returns:
            A list of dictionaries that represent the schema.
        """

        job_collection = self.bigquery.jobs()
        query_reply = self._get_query_results(
            job_collection, self.project_id, job_id, offset=0, limit=0)

        if not query_reply['jobComplete']:
            logging.warning('BigQuery job %s not complete' % job_id)
            return []

        return query_reply['schema']['fields']

    def check_job(self, job_id):
        """Return the state and number of results of a query by job id.

        Args:
            job_id: The job id of the query to check.

        Returns:
            Whether or not the query has completed and the total number of rows
            included in the query table if it has completed.
        """

        job_collection = self.bigquery.jobs()
        query_reply = self._get_query_results(
            job_collection, self.project_id, job_id, offset=0, limit=0)

        return (query_reply.get('jobComplete', False),
                query_reply.get('totalRows', 0))

    def get_query_rows(self, job_id, offset=None, limit=None):
        """Retrieve a list of rows from a query table by job id.

        Args:
            job_id: The job id that references a BigQuery query.
            offset: The offset of the rows to pull from BigQuery.
            limit: The number of rows to retrieve from a query table.

        Returns:
            A list of dictionaries that represent table rows.
        """

        job_collection = self.bigquery.jobs()
        query_reply = self._get_query_results(
            job_collection, self.project_id, job_id, offset=offset,
            limit=limit)

        if not query_reply['jobComplete']:
            logging.warning('BigQuery job %s not complete' % job_id)
            return []

        schema = query_reply['schema']['fields']
        rows = query_reply.get('rows', [])

        return [self._transform_row(row, schema) for row in rows]

    def get_tables(self, dataset_id, app_id, start_time, end_time):
        """Retrieve a list of tables that are related to the given app id
        and are inside the range of start and end times.

        Args:
            dataset_id: The BigQuery dataset id to consider.
            app_id: The appspot name
            start_time: The unix time after which records will be fetched.
            end_time: The unix time up to which records will be fetched.

        Returns:
            A list of table names.
        """

        every_table = self._get_all_tables(dataset_id)
        app_tables = every_table.get(app_id, {})

        return self._filter_tables_by_time(app_tables, start_time, end_time)

    def _get_all_tables(self, dataset_id):
        """Retrieve a list of all tables for the dataset.

        Args:
            dataset_id: the dataset to retrieve table names for.

        Returns:
            a dictionary of app ids mapped to their table names.
        """

        result = self.bigquery.tables().list(
            projectId=self.project_id,
            datasetId=dataset_id).execute()

        return self._parse_list_response(result)

    def _parse_list_response(self, list_response):
        """Parse the response received from calling list on tables.

        Args:
            list_response: The response found by calling list on a BigQuery
                           table object.

        Returns:
            The dictionary of dates referenced by table names.
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
            unix_seconds = int(time.mktime(table_date.timetuple()))
            tables[app_id].update({table_id: unix_seconds})

        # Turn off defualting
        tables.default_factory = None

        return tables

    def _parse_table_name(self, table_id):
        """Parse a table name in the form of appid_YYYY_MM or
        YYYY_MM_appid and return a tuple consisting of YYYY-MM and the app id.

        Args:
            table_id: The table id as listed by BigQuery.

        Returns:
            Tuple containing year/month and app id. Returns None, None if the
            table id cannot be parsed.
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

        Args:
            tables: The dictionary of dates referenced by table names
            start_time: The unix time after which records will be fetched.
            end_time: The unix time up to which records will be fetched.

        Returns:
            A list of table names that are inside the time range.
        """

        return [table_name for (table_name, unix_seconds) in tables.iteritems()
                if self._in_range(start_time, end_time, unix_seconds)]

    def _in_range(self, start_time, end_time, time):
        """Indicate if the given time falls inside of the given range.

        Args:
            start_time: The unix time for the start of the range.
            end_time: The unix time for the end of the range.
            time: The unix time to check.

        Returns:
            True if the time falls within the range, False otherwise.
        """

        ONE_MONTH = 2764800  # 32 days

        return start_time <= time <= end_time or \
            time <= start_time <= time + ONE_MONTH or \
            time <= end_time <= time + ONE_MONTH

    def _get_query_results(self, job_collection, project_id, job_id,
                           offset=None, limit=None):
        """Execute the query job indicated by the given job id.

        Args:
            job_collection: The collection the job belongs to.
            project_id: The project id of the table.
            job_id: The job id of the query to check.
            offset: The index the result set should start at.
            limit: The maximum number of results to retrieve.

        Returns:
            The query reply.
        """

        return job_collection.getQueryResults(
            projectId=project_id,
            jobId=job_id,
            startIndex=offset,
            maxResults=limit,
            timeoutMs=0).execute()

    def _transform_row(self, row, schema):
        """Apply the given schema to the given BigQuery data row.

        Args:
            row: A single BigQuery row to transform.
            schema: The BigQuery table schema to apply to the row, specifically
                    the list of field dicts.

        Returns:
            Dict containing keys that match the schema and values that match
            the row.
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

            log[col_name] = row_value

        return log

    def _recurse_on_row(self, col_dict, nested_value):
        """Apply the schema specified by the given dict to the nested value by
        recursing on it.

        Args:
            col_dict: A dict containing the schema to apply to the nested
                      value.
            nested_value: A value nested in a BigQuery row.
        Returns:
            Dict or list of dicts from applied schema.
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

