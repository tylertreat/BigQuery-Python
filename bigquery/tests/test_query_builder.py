import six
import unittest

unittest.TestCase.maxDiff = None


class TestRenderSelect(unittest.TestCase):

    def test_multiple_selects(self):
        """Ensure that render select can handle multiple selects."""
        from bigquery.query_builder import _render_select

        result = _render_select({
            'start_time': {'alias': 'TimeStamp'},
            'max_log_level': {'alias': 'MaxLogLevel'},
            'user': {'alias': 'User'},
            'status': {'alias': 'Status'},
            'resource': {'alias': 'URL'},
            'version_id': {'alias': 'Version'},
            'latency': {'alias': 'Latency'},
            'ip': {'alias': 'IP'},
            'app_logs': {'alias': 'AppLogs'}})

        expected = ('SELECT status as Status, latency as Latency, '
                    'max_log_level as MaxLogLevel, resource as URL, user as '
                    'User, ip as IP, start_time as TimeStamp, version_id as '
                    'Version, app_logs as AppLogs')
        six.assertCountEqual(
            self, sorted(expected[len('SELECT '):].split(', ')),
            sorted(result[len('SELECT '):].split(', ')))

    def test_casting(self):
        """Ensure that render select can handle custom casting."""
        from bigquery.query_builder import _render_select

        result = _render_select({
            'start_time': {
                'alias': 'TimeStamp',
                'format': 'SEC_TO_MICRO-INTEGER-FORMAT_UTC_USEC'
            }
        })

        self.assertEqual(
            result,
            'SELECT FORMAT_UTC_USEC(INTEGER(start_time*1000000)) as TimeStamp')

    def test_no_selects(self):
        """Ensure that render select can handle being passed no selects."""
        from bigquery.query_builder import _render_select

        result = _render_select({})

        self.assertEqual(result, 'SELECT *')


class TestRenderSources(unittest.TestCase):

    def test_multi_tables(self):
        """Ensure that render sources can handle multiple sources."""
        from bigquery.query_builder import _render_sources

        result = _render_sources('spider', ['man', 'pig', 'bro'])

        self.assertEqual(
            result, 'FROM [spider.man], [spider.pig], [spider.bro]')

    def test_no_tables(self):
        """Ensure that render sources can handle no tables."""
        from bigquery.query_builder import _render_sources

        result = _render_sources('spider', [])

        self.assertEqual(result, 'FROM ')

    def test_no_dataset(self):
        """Ensure that render sources can handle no data sets."""
        from bigquery.query_builder import _render_sources

        result = _render_sources('', ['man', 'pig', 'bro'])

        self.assertEqual(result, 'FROM [.man], [.pig], [.bro]')

    def test_tables_in_date_range(self):
        """Ensure that render sources can handle tables in DATE RANGE."""
        from bigquery.query_builder import _render_sources

        tables = {
            'date_range': True,
            'from_date': '2015-08-23',
            'to_date': '2015-10-10',
            'table': 'pets_'
        }

        result = _render_sources('animals', tables)

        self.assertEqual(result, "FROM (TABLE_DATE_RANGE([animals.pets_], "
                         "TIMESTAMP('2015-08-23'), TIMESTAMP('2015-10-10'))) ")


class TestRenderConditions(unittest.TestCase):

    def test_single_condition(self):
        """Ensure that render conditions can handle a single condition."""
        from bigquery.query_builder \
            import _render_conditions

        result = _render_conditions([
            {
                'field': 'bar',
                'type': 'STRING',
                'comparators': [
                    {'condition': '>=', 'negate': False, 'value': '1'}
                ]
            }
        ])

        self.assertEqual(result, "WHERE (bar >= STRING('1'))")

    def test_multiple_conditions(self):
        """Ensure that render conditions can handle multiple conditions."""
        from bigquery.query_builder \
            import _render_conditions

        result = _render_conditions([
            {
                'field': 'a',
                'type': 'STRING',
                'comparators': [
                    {'condition': '>=', 'negate': False, 'value': 'foobar'}
                ]
            },
            {
                'field': 'b',
                'type': 'INTEGER',
                'comparators': [
                    {'condition': '==', 'negate': True, 'value': '1'}
                ]
            },
            {
                'field': 'c',
                'type': 'STRING',
                'comparators': [
                    {'condition': '!=', 'negate': False, 'value': 'Shark Week'}
                ]
            },
        ])

        self.assertEqual(result, "WHERE (a >= STRING('foobar')) AND "
                                 "(NOT b == INTEGER('1')) AND "
                                 "(c != STRING('Shark Week'))")

    def test_multiple_comparators(self):
        """Ensure that render conditions can handle a multiple comparators."""
        from bigquery.query_builder \
            import _render_conditions

        result = _render_conditions([
            {
                'field': 'foobar',
                'type': 'STRING',
                'comparators': [
                    {'condition': '>=', 'negate': False, 'value': 'a'},
                    {'condition': '==', 'negate': False, 'value': 'b'},
                    {'condition': '<=', 'negate': False, 'value': 'c'},
                    {'condition': '>=', 'negate': True, 'value': 'd'},
                    {'condition': '==', 'negate': True, 'value': 'e'},
                    {'condition': '<=', 'negate': True, 'value': 'f'}
                ]
            }
        ])

        self.assertEqual(result, "WHERE ((foobar >= STRING('a') AND "
                                 "foobar == STRING('b') AND foobar <= "
                                 "STRING('c')) AND (NOT foobar >= "
                                 "STRING('d') AND NOT foobar == STRING('e') "
                                 "AND NOT foobar <= STRING('f')))")

    def test_boolean_field_type(self):
        """Ensure that render conditions can handle a boolean field."""
        from bigquery.query_builder \
            import _render_conditions

        result = _render_conditions([
            {
                'field': 'foobar',
                'type': 'BOOLEAN',
                'comparators': [
                    {'condition': '==', 'negate': False, 'value': True},
                    {'condition': '!=', 'negate': False, 'value': False},
                    {'condition': '==', 'negate': False, 'value': 'a'},
                    {'condition': '!=', 'negate': False, 'value': ''},
                    {'condition': '==', 'negate': True, 'value': 100},
                    {'condition': '!=', 'negate': True, 'value': 0},
                ]
            }
        ])

        self.assertEqual(result, "WHERE ((foobar == BOOLEAN(1) AND "
                                 "foobar != BOOLEAN(0) AND foobar == "
                                 "BOOLEAN(1) AND foobar != BOOLEAN(0)) AND "
                                 "(NOT foobar == BOOLEAN(1) AND NOT "
                                 "foobar != BOOLEAN(0)))")

    def test_in_comparator(self):
        """Ensure that render conditions can handle "IN" condition."""
        from bigquery.query_builder \
            import _render_conditions

        result = _render_conditions([
            {
                'field': 'foobar',
                'type': 'STRING',
                'comparators': [
                    {'condition': 'IN', 'negate': False, 'value': ['a', 'b']},
                    {'condition': 'IN', 'negate': False, 'value': {'c', 'd'}},
                    {'condition': 'IN', 'negate': False, 'value': ('e', 'f')},
                    {'condition': 'IN', 'negate': False, 'value': 'g'},
                    {'condition': 'IN', 'negate': True, 'value': ['h', 'i']},
                    {'condition': 'IN', 'negate': True, 'value': {'j', 'k'}},
                    {'condition': 'IN', 'negate': True, 'value': ('l', 'm')},
                    {'condition': 'IN', 'negate': True, 'value': 'n'},
                ]
            }
        ])

        six.assertCountEqual(self, result[len('WHERE '):].split(' AND '),
                             "WHERE ((foobar IN (STRING('a'), STRING('b'))"
                             " AND foobar IN (STRING('c'), STRING('d')) "
                             "AND foobar IN (STRING('e'), STRING('f')) AND"
                             " foobar IN (STRING('g'))) AND (NOT foobar IN"
                             " (STRING('h'), STRING('i')) AND NOT foobar "
                             "IN (STRING('j'), STRING('k')) AND NOT foobar"
                             " IN (STRING('l'), STRING('m')) AND NOT "
                             "foobar IN (STRING('n'))))" [len('WHERE '):]
                             .split(' AND '))

    def test_between_comparator(self):
        """Ensure that render conditions can handle "BETWEEN" condition."""
        from bigquery.query_builder import _render_conditions

        result = _render_conditions([
            {
                'field': 'foobar',
                'type': 'STRING',
                'comparators': [
                    {'condition': 'BETWEEN', 'negate': False,
                        'value': ['a', 'b']},
                    {'condition': 'BETWEEN', 'negate': False,
                        'value': {'c', 'd'}},
                    {'condition': 'BETWEEN', 'negate': False,
                        'value': ('e', 'f')},
                    {'condition': 'BETWEEN', 'negate': True,
                        'value': ['h', 'i']},
                    {'condition': 'BETWEEN', 'negate': True,
                        'value': {'j', 'k'}},
                    {'condition': 'BETWEEN', 'negate': True,
                        'value': ('l', 'm')}
                ]
            }
        ])

        six.assertCountEqual(self, result[len('WHERE '):].split(' AND '),
                             "WHERE ((foobar BETWEEN STRING('a') AND "
                             "STRING('b') AND foobar BETWEEN STRING('c') "
                             "AND STRING('d') AND foobar BETWEEN "
                             "STRING('e') AND STRING('f')) AND (NOT foobar "
                             "BETWEEN STRING('h') AND STRING('i') AND NOT "
                             "foobar BETWEEN STRING('j') AND STRING('k') "
                             "AND NOT foobar BETWEEN STRING('l') AND "
                             "STRING('m')))" [len('WHERE '):]
                             .split(' AND '))


class TestRenderOrder(unittest.TestCase):

    def test_order(self):
        """Ensure that render order can work under expected conditions."""
        from bigquery.query_builder import _render_order

        result = _render_order({'fields': ['foo'], 'direction': 'desc'})

        self.assertEqual(result, "ORDER BY foo desc")

    def test_no_order(self):
        """Ensure that render order can work with out any arguments."""
        from bigquery.query_builder import _render_order

        result = _render_order(None)

        self.assertEqual(result, "")


class TestGroupings(unittest.TestCase):

    def test_mutliple_fields(self):
        """Ensure that render grouping works with multiple fields."""
        from bigquery.query_builder \
            import _render_groupings

        result = _render_groupings(['foo', 'bar', 'shark'])

        self.assertEqual(result, "GROUP BY foo, bar, shark")

    def test_no_fields(self):
        """Ensure that render groupings can work with out any arguments."""
        from bigquery.query_builder \
            import _render_groupings

        result = _render_groupings(None)

        self.assertEqual(result, "")


class TestRenderHaving(unittest.TestCase):

    def test_mutliple_fields(self):
        """Ensure that render having works with multiple fields."""
        from bigquery.query_builder \
            import _render_having

        result = _render_having([
            {
                'field': 'bar',
                'type': 'STRING',
                'comparators': [
                    {'condition': '>=', 'negate': False, 'value': '1'}
                ]
            }
        ])

        self.assertEqual(result, "HAVING (bar >= STRING('1'))")

    def test_no_fields(self):
        """Ensure that render having can work with out any arguments."""
        from bigquery.query_builder \
            import _render_having

        result = _render_having(None)

        self.assertEqual(result, "")


class TestRenderQuery(unittest.TestCase):

    def test_full_query(self):
        """Ensure that all the render query arguments work together."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {
                    'field': 'start_time',
                    'comparators': [
                        {
                            'condition': '<=',
                            'value': 1371566954,
                            'negate': False
                        }
                    ],
                    'type': 'INTEGER'
                },
                {
                    'field': 'start_time',
                    'comparators': [
                        {
                            'condition': '>=',
                            'value': 1371556954,
                            'negate': False
                        }
                    ],
                    'type': 'INTEGER'
                }
            ],
            groupings=['timestamp', 'status'],
            having=[
                {
                    'field': 'status',
                    'comparators': [
                        {
                            'condition': '==',
                            'value': 1,
                            'negate': False
                        }
                    ],
                    'type': 'INTEGER'
                }
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM [dataset.2013_06_appspot_1]"
                          " WHERE (start_time <= INTEGER('1371566954')) AND "
                          "(start_time >= INTEGER('1371556954')) GROUP BY "
                          "timestamp, status HAVING (status == INTEGER('1')) "
                          "ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_empty_conditions(self):
        """Ensure that render query can handle an empty list of conditions."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1]    ORDER BY "
                          "timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))

        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_incorrect_conditions(self):
        """Ensure that render query can handle incorrectly formatted
        conditions.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'asdfasdfasdf': 'start_time', 'ffd': 1371566954, 'comparator':
                    '<=', 'type': 'INTEGER'},
                {'field': 'start_time', 'value': {'value': 1371556954,
                                                  'negate': False},
                 'compoorattor': '>=', 'type': 'INTEGER'}
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1]    ORDER BY "
                          "timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_multiple_condition_values(self):
        """Ensure that render query can handle conditions with multiple values.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'resource', 'comparators': [{'condition': 'CONTAINS',
                                                       'value': 'foo',
                                                       'negate': False},
                                                      {'condition': 'CONTAINS',
                                                       'value': 'bar',
                                                       'negate': True},
                                                      {'condition': 'CONTAINS',
                                                       'value': 'baz',
                                                       'negate': False}],
                 'type': 'STRING'}
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954')) AND "
                          "((resource CONTAINS STRING('foo') AND resource "
                          "CONTAINS STRING('baz')) AND (NOT resource CONTAINS "
                          "STRING('bar')))   ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_negated_condition_value(self):
        """Ensure that render query can handle conditions with negated values.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'resource', 'comparators': [{'condition': 'CONTAINS',
                                                       'value': 'foo',
                                                       'negate': True}],
                 'type': 'STRING'}
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (NOT resource "
                          "CONTAINS STRING('foo'))   ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_multiple_negated_condition_values(self):
        """Ensure that render query can handle conditions with multiple negated
        values.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'resource', 'comparators': [{'condition': 'CONTAINS',
                                                       'value': 'foo',
                                                       'negate': True},
                                                      {'condition': 'CONTAINS',
                                                       'value': 'baz',
                                                       'negate': True},
                                                      {'condition': 'CONTAINS',
                                                       'value': 'bar',
                                                       'negate': True}],
                 'type': 'STRING'}
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (NOT resource "
                          "CONTAINS STRING('foo') AND NOT resource CONTAINS "
                          "STRING('baz') AND NOT resource CONTAINS "
                          "STRING('bar'))   ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_empty_order(self):
        """Ensure that render query can handle an empty formatted order."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954'))   ")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_incorrect_order(self):
        """Ensure that render query can handle inccorectly formatted order."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'feeld': 'timestamp', 'dir': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954'))   ")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_empty_select(self):
        """Ensure that render query corrently handles no selection."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={},
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT * FROM [dataset.2013_06_appspot_1] "
                          "WHERE (start_time <= INTEGER('1371566954')) AND "
                          "(start_time >= INTEGER('1371556954'))   ORDER BY "
                          "timestamp desc")
        self.assertEqual(result, expected_query)

    def test_no_alias(self):
        """Ensure that render query runs without an alias for a select."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {},
                'status': {},
                'resource': {}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'}
            ],
            order_by={'fields': ['start_time'], 'direction': 'desc'})

        expected_query = ("SELECT status , start_time , resource  FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954'))   ORDER BY start_time desc")
        expected_select = (field.strip() for field in
                           expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = (expected_query[len('SELECT '):].split('FROM')[1]
                         .strip())
        result_select = (field.strip() for field in
                         result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1].strip()
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_formatting(self):
        """Ensure that render query runs with formatting a select."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {
                    'alias': 'timestamp',
                    'format': 'INTEGER-FORMAT_UTC_USEC'
                },
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, "
                          "FORMAT_UTC_USEC(INTEGER(start_time)) as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954'))   ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_formatting_duplicate_columns(self):
        """Ensure that render query runs with formatting a select for a
        column selected twice.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': [
                    {
                        'alias': 'timestamp',
                        'format': 'INTEGER-FORMAT_UTC_USEC'
                    },
                    {
                        'alias': 'day',
                        'format': ('SEC_TO_MICRO-INTEGER-'
                                   'FORMAT_UTC_USEC-LEFT:10')
                    }
                ],
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, "
                          "FORMAT_UTC_USEC(INTEGER(start_time)) as timestamp, "
                          "LEFT(FORMAT_UTC_USEC(INTEGER(start_time*1000000)),"
                          "10) as day, resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE "
                          "(start_time <= INTEGER('1371566954')) AND "
                          "(start_time >= INTEGER('1371556954'))   ORDER BY "
                          "timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_sec_to_micro_formatting(self):
        """Ensure that render query runs sec_to_micro formatting on a
        select.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {
                    'alias': 'timestamp',
                    'format': 'SEC_TO_MICRO-INTEGER-SEC_TO_TIMESTAMP'
                },
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, "
                          "SEC_TO_TIMESTAMP(INTEGER(start_time*1000000)) as "
                          "timestamp, resource as url FROM "
                          "[dataset.2013_06_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954'))   ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_no_table_or_dataset(self):
        """Ensure that render query returns None if there is no dataset or
        table.
        """
        from bigquery.query_builder import render_query

        result = render_query(
            dataset=None,
            tables=None,
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        self.assertIsNone(result)

    def test_empty_groupings(self):
        """Ensure that render query can handle an empty list of groupings."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            groupings=[],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1]    ORDER BY "
                          "timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)

    def test_multi_tables(self):
        """Ensure that render query arguments work with multiple tables."""
        from bigquery.query_builder import render_query

        result = render_query(
            dataset='dataset',
            tables=['2013_06_appspot_1', '2013_07_appspot_1'],
            select={
                'start_time': {'alias': 'timestamp'},
                'status': {'alias': 'status'},
                'resource': {'alias': 'url'}
            },
            conditions=[
                {'field': 'start_time', 'comparators': [{'condition': '<=',
                                                         'value': 1371566954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
                {'field': 'start_time', 'comparators': [{'condition': '>=',
                                                         'value': 1371556954,
                                                         'negate': False}],
                 'type': 'INTEGER'},
            ],
            groupings=['timestamp', 'status'],
            order_by={'fields': ['timestamp'], 'direction': 'desc'})

        expected_query = ("SELECT status as status, start_time as timestamp, "
                          "resource as url FROM "
                          "[dataset.2013_06_appspot_1], "
                          "[dataset.2013_07_appspot_1] WHERE (start_time "
                          "<= INTEGER('1371566954')) AND (start_time >= "
                          "INTEGER('1371556954')) GROUP BY timestamp, status  "
                          "ORDER BY timestamp desc")
        expected_select = (expected_query[len('SELECT '):]
                           .split('FROM')[0].strip().split(', '))
        expected_from = expected_query[len('SELECT '):].split('FROM')[1]
        result_select = (result[len('SELECT '):].split('FROM')[0]
                         .strip().split(', '))
        result_from = result[len('SELECT '):].split('FROM')[1]
        six.assertCountEqual(self, expected_select, result_select)
        six.assertCountEqual(self, expected_from, result_from)
