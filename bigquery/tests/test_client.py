import unittest

import mock

from bigquery import client


class TestGetClient(unittest.TestCase):

    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.client = client.BigQueryClient(self.mock_bq_service, 'project')

    def test_no_credentials(self):
        """Ensure an Exception is raised when no credentials are provided."""

        self.assertRaises(Exception, client.get_client, 'foo', 'bar')

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_readonly(self, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read-only permissions.
        """
        from bigquery.client import BIGQUERY_SCOPE_READ_ONLY

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_cred.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        key = 'key'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_account=service_account, private_key=key,
            readonly=True)

        mock_return_cred.assert_called_once_with()
        mock_cred.assert_called_once_with(service_account, key,
                                          scope=BIGQUERY_SCOPE_READ_ONLY)
        mock_cred.authorize.assert_called_once()
        mock_build.assert_called_once_with('bigquery', 'v2', http=mock_http)
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_read_write(self, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions.
        """
        from bigquery.client import BIGQUERY_SCOPE

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_cred.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        key = 'key'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_account=service_account, private_key=key,
            readonly=False)

        mock_return_cred.assert_called_once_with()
        mock_cred.assert_called_once_with(service_account, key,
                                          scope=BIGQUERY_SCOPE)
        mock_cred.authorize.assert_called_once()
        mock_build.assert_called_once_with('bigquery', 'v2', http=mock_http)
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)


class TestQuery(unittest.TestCase):

    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.query = 'foo'
        self.project_id = 'project'
        self.client = client.BigQueryClient(self.mock_bq_service,
                                            self.project_id)

    def test_query(self):
        """Ensure that we retrieve the job id from the query."""

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref
        }

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 10000, 'dryRun': False,
                  'maxResults': None}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

    def test_query_max_results(self):
        """Ensure that we retrieve the job id from the query and the maxResults
        parameter is set.
        """

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref
        }

        self.mock_job_collection.query.return_value = mock_query_job
        max_results = 10

        job_id, results = self.client.query(self.query,
                                            max_results=max_results)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 10000,
                  'maxResults': max_results, 'dryRun': False}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

    def test_query_timeout(self):
        """Ensure that we retrieve the job id from the query and the timeoutMs
        parameter is set correctly.
        """

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref
        }

        self.mock_job_collection.query.return_value = mock_query_job
        timeout = 5

        job_id, results = self.client.query(self.query, timeout=timeout)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': timeout * 1000, 'dryRun':
                  False, 'maxResults': None}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

    def test_query_dry_run_valid(self):
        """Ensure that None and an empty list is returned from the query when
        dry_run is True and the query is valid.
        """

        mock_query_job = mock.Mock()

        mock_query_job.execute.return_value = {'jobReference': {}}

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query, dry_run=True)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 10000, 'maxResults': None,
                  'dryRun': True}
        )
        self.assertIsNone(job_id)
        self.assertEqual([], results)

    def test_query_dry_run_invalid(self):
        """Ensure that None and a dict is returned from the query when dry_run
        is True and the query is invalid.
        """
        from apiclient.errors import HttpError

        mock_query_job = mock.Mock()

        mock_query_job.execute.side_effect = HttpError(
            'crap', '{"message": "Bad query"}')

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query('%s blah' % self.query,
                                            dry_run=True)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': '%s blah' % self.query, 'timeoutMs': 10000,
                  'maxResults': None,
                  'dryRun': True}
        )
        self.assertIsNone(job_id)
        self.assertEqual({'message': 'Bad query'}, results)

    def test_query_with_results(self):
        """Ensure that we retrieve the job id from the query and results if
        they are available.
        """

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref,
            'schema': {'fields': [{'name': 'foo', 'type': 'INTEGER'}]},
            'rows': [{'f': [{'v': 10}]}],
        }

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 10000, 'dryRun': False,
                  'maxResults': None}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [{'foo': 10}])


class TestGetQueryResults(unittest.TestCase):

    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.project_id = 'project'
        self.client = client.BigQueryClient(self.mock_bq_service,
                                            self.project_id)

    def test_get_response(self):
        """Ensure that the query is executed and the query reply is returned.
        """

        project_id = 'foo'
        job_id = 'bar'

        mock_query_job = mock.Mock()
        mock_query_reply = mock.Mock()
        mock_query_job.execute.return_value = mock_query_reply
        self.mock_job_collection.getQueryResults.return_value = mock_query_job

        offset = 5
        limit = 10

        actual = self.client._get_query_results(self.mock_job_collection,
                                                project_id, job_id,
                                                offset, limit)

        self.mock_job_collection.getQueryResults.assert_called_once_with(
            timeoutMs=0, projectId=project_id, jobId=job_id,
            startIndex=offset, maxResults=limit)
        mock_query_job.execute.assert_called_once()
        self.assertEquals(actual, mock_query_reply)


class TestTransformRow(unittest.TestCase):

    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.project_id = 'project'
        self.client = client.BigQueryClient(self.mock_bq_service,
                                            self.project_id)

    def test_transform_row(self):
        """Ensure that the row dict is correctly transformed to a log dict."""

        schema = [{'name': 'foo', 'type': 'INTEGER'},
                  {'name': 'bar', 'type': 'FLOAT'},
                  {'name': 'baz', 'type': 'STRING'},
                  {'name': 'qux', 'type': 'BOOLEAN'},
                  {'name': 'timestamp', 'type': 'FLOAT'}]

        row = {'f': [{'v': '42'}, {'v': None}, {'v': 'batman'},
                     {'v': 'True'}, {'v': '1.371145650319132E9'}]}

        expected = {'foo': 42, 'bar': None, 'baz': 'batman', 'qux': True,
                    'timestamp': 1371145650.319132}

        actual = self.client._transform_row(row, schema)

        self.assertEquals(actual, expected)

    def test_transform_row_with_nested(self):
        """Ensure that the row dict with nested records is correctly
        transformed to a log dict.
        """

        schema = [{'name': 'foo', 'type': 'INTEGER'},
                  {'name': 'bar', 'type': 'FLOAT'},
                  {'name': 'baz', 'type': 'STRING'},
                  {'name': 'qux', 'type': 'RECORD', 'mode': 'SINGLE',
                   'fields': [{'name': 'foobar', 'type': 'INTEGER'},
                              {'name': 'bazqux', 'type': 'STRING'}]}]

        row = {'f': [{'v': '42'}, {'v': '36.98'}, {'v': 'batman'},
                     {'v': {'f': [{'v': '120'}, {'v': 'robin'}]}}]}
        expected = {'foo': 42, 'bar': 36.98, 'baz': 'batman',
                    'qux': {'foobar': 120, 'bazqux': 'robin'}}

        actual = self.client._transform_row(row, schema)

        self.assertEquals(actual, expected)

    def test_transform_row_with_nested_repeated(self):
        """Ensure that the row dict with nested repeated records is correctly
        transformed to a log dict.
        """

        schema = [{'name': 'foo', 'type': 'INTEGER'},
                  {'name': 'bar', 'type': 'FLOAT'},
                  {'name': 'baz', 'type': 'STRING'},
                  {'name': 'qux', 'type': 'RECORD', 'mode': 'REPEATED',
                   'fields': [{'name': 'foobar', 'type': 'INTEGER'},
                              {'name': 'bazqux', 'type': 'STRING'}]}]

        row = {'f': [{'v': '42'}, {'v': '36.98'}, {'v': 'batman'},
                     {'v': [{'v': {'f': [{'v': '120'}, {'v': 'robin'}]}},
                            {'v': {'f': [{'v': '300'}, {'v': 'joker'}]}}]}]}
        expected = {'foo': 42, 'bar': 36.98, 'baz': 'batman',
                    'qux': [{'foobar': 120, 'bazqux': 'robin'},
                            {'foobar': 300, 'bazqux': 'joker'}]}

        actual = self.client._transform_row(row, schema)

        self.assertEquals(actual, expected)


@mock.patch('bigquery.client.BigQueryClient._get_query_results')
class TestCheckJob(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.project_id = 'project'
        self.client = client.BigQueryClient(mock.Mock(), self.project_id)

    def test_job_incomplete(self, mock_exec):
        """Ensure that we return None if the job is not yet complete."""

        mock_exec.return_value = {'jobComplete': False}

        is_completed, total_rows = self.client.check_job(1)

        self.assertFalse(is_completed)
        self.assertEquals(total_rows, 0)

    def test_query_complete(self, mock_exec):
        """Ensure that we can handle a normal query result."""

        mock_exec.return_value = {
            'jobComplete': True,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': 2
        }

        is_completed, total_rows = self.client.check_job(1)

        self.assertTrue(is_completed)
        self.assertEquals(total_rows, 2)


class TestFilterTablesByTime(unittest.TestCase):

    def test_empty_tables(self):
        """Ensure we can handle filtering an empty dictionary"""

        bq = client.BigQueryClient(None, 'project')

        tables = bq._filter_tables_by_time({}, 1370000000, 0)

        self.assertEqual([], tables)

    def test_multi_inside_range(self):
        """Ensure we can correctly filter several application ids"""

        bq = client.BigQueryClient(None, 'project')

        tables = bq._filter_tables_by_time({
            'Spider-Man': 1370002001,
            'Daenerys Targaryen': 1370001999,
            'Gordon Freeman': 1369999999,
            'William Shatner': 1370001000,
            'Heavy Weapons Guy': 0
        }, 1370002000, 1370000000)

        self.assertEqual([
            'Daenerys Targaryen', 'William Shatner', 'Gordon Freeman'], tables)

    def test_not_inside_range(self):
        """Ensure we can correctly filter several application ids outside the
        range we are searching for.
        """

        bq = client.BigQueryClient(None, 'project')

        tables = bq._filter_tables_by_time({
            'John Snow': 9001,
            'Adam West': 100000000000000,
            'Glados': -1,
            'Potato': 0,
        }, 1370002000, 1370000000)

        self.assertEqual([], tables)


FULL_LIST_RESPONSE = {
    "kind": "bigquery#tableList",
    "etag": "\"GSclnjk0zID1ucM3F-xYinOm1oE/cn58Rpu8v8pB4eoJQaiTe11lPQc\"",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_05_appspot_1",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_05_appspot"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_1",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_1"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_2",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_2"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_3",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_3"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_4",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_4"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_5",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_5"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.appspot_6_2013_06",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "appspot_6_2013_06"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "bad table data"
        }
    ],
    "totalItems": 8
}


@mock.patch('bigquery.client.BigQueryClient._get_query_results')
class TestGetQuerySchema(unittest.TestCase):

    def test_query_complete(self, get_query_mock):
        """Ensure that get_query_schema works when a query is complete."""
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': True,
            'schema': {'fields': 'This is our schema'}
        }

        result_schema = bq.get_query_schema(job_id=123)

        self.assertEquals(result_schema, 'This is our schema')

    def test_query_incomplete(self, get_query_mock):
        """Ensure that get_query_schema handles scenarios where the query
        is not finished.
        """
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': False,
            'schema': {'fields': 'This is our schema'}
        }

        result_schema = bq.get_query_schema(job_id=123)

        self.assertEquals(result_schema, [])


class TestGetTableSchema(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_table_exists(self):
        """Ensure that the table schema is returned if the table exists."""

        expected = [
            {'type': 'FLOAT', 'name': 'foo', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'bar', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'baz', 'mode': 'NULLABLE'},
        ]

        self.mock_tables.get.return_value.execute.return_value = \
            {'schema': {'fields': expected}}

        self.assertEqual(
            expected, self.client.get_table_schema(self.dataset, self.table))
        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, tableId=self.table, datasetId=self.dataset)
        self.mock_tables.get.return_value.execute.assert_called_once_with()

    def test_table_does_not_exist(self):
        """Ensure that None is returned if the table doesn't exist."""
        from apiclient.errors import HttpError

        self.mock_tables.get.return_value.execute.side_effect = \
            HttpError({'status': "404"}, '{}')

        self.assertIsNone(
            self.client.get_table_schema(self.dataset, self.table))
        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, tableId=self.table, datasetId=self.dataset)
        self.mock_tables.get.return_value.execute.assert_called_once_with()


@mock.patch('bigquery.client.BigQueryClient._get_query_results')
class TestGetQueryRows(unittest.TestCase):

    def test_query_complete(self, get_query_mock):
        """Ensure that get_query_rows works when a query is complete."""
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': True,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': 2
        }

        result_rows = bq.get_query_rows(job_id=123, offset=0, limit=0)

        expected_rows = [{'foo': 'bar', 'spider': 'man'},
                         {'foo': 'abc', 'spider': 'xyz'}]
        self.assertEquals(result_rows, expected_rows)

    def test_query_incomplete(self, get_query_mock):
        """Ensure that get_query_rows handles scenarios where the query is not
        finished.
        """
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': False,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': 2
        }

        result_rows = bq.get_query_rows(job_id=123, offset=0, limit=0)

        expected_rows = []
        self.assertEquals(result_rows, expected_rows)


class TestCheckTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_table_does_not_exist(self):
        """Ensure that if the table does not exist, False is returned."""

        self.mock_tables.get.return_value.execute.side_effect = Exception()

        actual = self.client.check_table(self.dataset, self.table)

        self.assertFalse(actual)

        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.get.return_value.execute.assert_called_once_with()

    def test_table_does_exist(self):
        """Ensure that if the table does exist, True is returned."""

        self.mock_tables.get.return_value.execute.side_effect = {
            'status': 'foo'}

        actual = self.client.check_table(self.dataset, self.table)

        self.assertTrue(actual)

        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.get.return_value.execute.assert_called_once_with()


class TestCreateTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.schema = [
            {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
        ]
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.body = {
            'schema': {'fields': self.schema},
            'tableReference': {
                'tableId': self.table, 'projectId': self.project,
                'datasetId': self.dataset}
        }

    def test_table_create_failed(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_tables.insert.return_value.execute.side_effect = Exception()

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertFalse(actual)

        self.mock_tables.insert.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_tables.insert.return_value.execute.assert_called_once_with()

    def test_table_create_success(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_tables.insert.return_value.execute.side_effect = {
            'status': 'foo'}

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertTrue(actual)

        self.mock_tables.insert.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_tables.insert.return_value.execute.assert_called_once_with()


class TestDeleteTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_delete_table_fail(self):
        """Ensure that if deleting table fails, False is returned."""

        self.mock_tables.delete.return_value.execute.side_effect = Exception()

        actual = self.client.delete_table(self.dataset, self.table)

        self.assertFalse(actual)

        self.mock_tables.delete.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.delete.return_value.execute.assert_called_once_with()

    def test_delete_table_success(self):
        """Ensure that if deleting table succeeds, True is returned."""

        self.mock_tables.delete.return_value.execute.side_effect = {
            'status': 'foo'}

        actual = self.client.delete_table(self.dataset, self.table)

        self.assertTrue(actual)

        self.mock_tables.delete.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.delete.return_value.execute.assert_called_once_with()


class TestParseListReponse(unittest.TestCase):

    def test_full_parse(self):
        """Ensures we can parse a full list response."""

        bq = client.BigQueryClient(None, 'project')

        tables = bq._parse_list_response(FULL_LIST_RESPONSE)

        expected_result = {
            'appspot-3': {'2013_06_appspot_3': 1370044800},
            'appspot-2': {'2013_06_appspot_2': 1370044800},
            'appspot-1': {'2013_06_appspot_1': 1370044800},
            'appspot-6': {'appspot_6_2013_06': 1370044800},
            'appspot-5': {'2013_06_appspot_5': 1370044800},
            'appspot-4': {'2013_06_appspot_4': 1370044800},
            'appspot': {'2013_05_appspot': 1367366400}
        }

        self.assertEquals(expected_result, tables)

    def test_empty_parse(self):
        """Ensures we can parse an empty dictionary."""

        bq = client.BigQueryClient(None, 'project')

        tables = bq._parse_list_response({})

        self.assertEquals(tables, {})

    def test_error(self):
        """Ensures we can handle parsing a response error."""

        error_response = {
            "error": {
                "errors": [
                    {
                        "domain": "global",
                        "reason": "required",
                        "message": "Login Required",
                        "locationType": "header",
                        "location": "Authorization"
                    }
                ],
                "code": 401,
                "message": "Login Required"
            }
        }
        bq = client.BigQueryClient(None, 'project')

        tables = bq._parse_list_response(error_response)

        self.assertEquals(tables, {})

    def test_incorrect_table_formats(self):
        """Ensures we can parse incorrectly formatted table ids."""

        list_response = {
            "tables": [
                {
                    "tableReference": {
                        "tableId": "somethingwrong"
                    }
                },
                {
                    "tableReference": {
                        "tableId": "john-snow"
                    }
                },
                {
                    "tableReference": {
                        "tableId": "'------',"
                    }
                },
                {
                    "tableReference": {
                        "tableId": ""
                    }
                },
                {
                    "tableReference": {
                        "tableId": "adam_west"
                    }
                }
            ],
        }
        bq = client.BigQueryClient(None, 'project')

        tables = bq._parse_list_response(list_response)

        self.assertEquals(tables, {})


class TestPushRows(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_table_data = mock.Mock()
        self.mock_bq_service.tabledata.return_value = self.mock_table_data
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.rows = [
            {'one': 'uno', 'two': 'dos'}, {'one': 'ein', 'two': 'zwei'}]
        self.data = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [{'insertId': row['one'], 'json': row} for row in
                     self.rows]
        }

    def test_push_failed(self):
        """Ensure that if insertAll does not raise an exception, but returns
        insertion errors, False is returned.
        """

        self.mock_table_data.insertAll.return_value.execute.return_value = {
            'insertErrors': 'foo'}

        actual = self.client.push_rows(self.rows, 'one', self.dataset,
                                       self.table)

        self.assertFalse(actual)

        self.mock_bq_service.tabledata.assert_called_once_with()

        self.mock_table_data.insertAll.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table,
            body=self.data)

        execute_calls = [mock.call()]
        self.mock_table_data.insertAll.return_value.execute.assert_has_calls(
            execute_calls)

    def test_push_exception(self):
        """Ensure that if insertAll raises an exception, False is returned."""

        self.mock_table_data.insertAll.return_value.execute.side_effect = \
            Exception()

        actual = self.client.push_rows(self.rows, 'one', self.dataset,
                                       self.table)

        self.assertFalse(actual)

        self.mock_bq_service.tabledata.assert_called_once_with()

        self.mock_table_data.insertAll.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table,
            body=self.data)

        execute_calls = [mock.call()]
        self.mock_table_data.insertAll.return_value.execute.assert_has_calls(
            execute_calls)

    def test_push_success(self):
        """Ensure that if insertAll does not raise an exception, but returns
        insertion errors, False is returned.
        """

        self.mock_table_data.insertAll.return_value.execute.return_value = {
            'status': 'foo'}

        actual = self.client.push_rows(self.rows, 'one', self.dataset,
                                       self.table)

        self.assertTrue(actual)

        self.mock_bq_service.tabledata.assert_called_once_with()

        self.mock_table_data.insertAll.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table,
            body=self.data)

        execute_calls = [mock.call()]
        self.mock_table_data.insertAll.return_value.execute.assert_has_calls(
            execute_calls)


class TestGetAllTables(unittest.TestCase):

    def test_get_tables(self):
        """Ensure get_all_tables fetches table names from BigQuery."""

        mock_execute = mock.Mock()
        mock_execute.execute.return_value = FULL_LIST_RESPONSE

        mock_tables = mock.Mock()
        mock_tables.list.return_value = mock_execute

        mock_bq_service = mock.Mock()
        mock_bq_service.tables.return_value = mock_tables

        bq = client.BigQueryClient(mock_bq_service, 'project')

        expected_result = {
            'appspot-3': {'2013_06_appspot_3': 1370044800},
            'appspot-2': {'2013_06_appspot_2': 1370044800},
            'appspot-1': {'2013_06_appspot_1': 1370044800},
            'appspot-6': {'appspot_6_2013_06': 1370044800},
            'appspot-5': {'2013_06_appspot_5': 1370044800},
            'appspot-4': {'2013_06_appspot_4': 1370044800},
            'appspot': {'2013_05_appspot': 1367366400}
        }

        tables = bq._get_all_tables('dataset')
        self.assertEquals(expected_result, tables)


class TestGetTables(unittest.TestCase):

    def test_get_tables(self):
        """Ensure tables falling in the time window are returned."""

        mock_execute = mock.Mock()
        mock_execute.execute.return_value = FULL_LIST_RESPONSE

        mock_tables = mock.Mock()
        mock_tables.list.return_value = mock_execute

        mock_bq_service = mock.Mock()
        mock_bq_service.tables.return_value = mock_tables

        bq = client.BigQueryClient(mock_bq_service, 'project')

        tables = bq.get_tables('dataset', 'appspot-1', 0, 10000000000)
        self.assertItemsEqual(tables, ['2013_06_appspot_1'])

    def test_get_tables_from_datetimes(self):
        """Ensure tables falling in the time window, specified with datetimes,
        are returned.
        """
        from datetime import datetime

        mock_execute = mock.Mock()
        mock_execute.execute.return_value = FULL_LIST_RESPONSE

        mock_tables = mock.Mock()
        mock_tables.list.return_value = mock_execute

        mock_bq_service = mock.Mock()
        mock_bq_service.tables.return_value = mock_tables

        bq = client.BigQueryClient(mock_bq_service, 'project')

        start = datetime(2013, 5, 10)
        end = datetime(2013, 7, 10)

        tables = bq.get_tables('dataset', 'appspot-1', start, end)
        self.assertItemsEqual(tables, ['2013_06_appspot_1'])

