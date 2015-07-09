from six.moves.builtins import object
from datetime import datetime
import unittest

import six
from bigquery.schema_builder import schema_from_record
from bigquery.schema_builder import describe_field
from bigquery.schema_builder import bigquery_type
from bigquery.schema_builder import InvalidTypeException


class TestBigQueryTypes(unittest.TestCase):

    def test_str_is_string(self):
        six.assertCountEqual(self, bigquery_type("Bob"), 'string')

    def test_unicode_is_string(self):
        six.assertCountEqual(self, bigquery_type(u"Here is a happy face \u263A"),
                             'string')

    def test_int_is_integer(self):
        six.assertCountEqual(self, bigquery_type(123), 'integer')

    def test_datetime_is_timestamp(self):
        six.assertCountEqual(self, bigquery_type(datetime.now()), 'timestamp')

    def test_isoformat_timestring(self):
        six.assertCountEqual(self, bigquery_type(datetime.now().isoformat()),
                             'timestamp')

    def test_timestring_feb_20_1973(self):
        six.assertCountEqual(self, bigquery_type("February 20th 1973"),
                             'timestamp')

    def test_timestring_thu_1_july_2004_22_30_00(self):
        six.assertCountEqual(self, bigquery_type("Thu, 1 July 2004 22:30:00"),
                             'timestamp')

    def test_today_is_not_timestring(self):
        six.assertCountEqual(self, bigquery_type("today"), 'string')

    def test_timestring_next_thursday(self):
        six.assertCountEqual(self, bigquery_type("February 20th 1973"), 'timestamp')

    def test_timestring_arbitrary_fn_success(self):
        six.assertCountEqual(
            self, bigquery_type("whatever", timestamp_parser=lambda x: True),
            'timestamp')

    def test_timestring_arbitrary_fn_fail(self):
        six.assertCountEqual(
            self, bigquery_type("February 20th 1973",
                                timestamp_parser=lambda x: False),
            'string')

    def test_class_instance_is_invalid_type(self):
        class SomeClass(object):
            pass

        self.assertIsNone(bigquery_type(SomeClass()))

    def test_list_is_invalid_type(self):
        self.assertIsNone(bigquery_type([1, 2, 3]))

    def test_dict_is_record(self):
        six.assertCountEqual(self, bigquery_type({"a": 1}), 'record')


class TestFieldDescription(unittest.TestCase):

    def test_simple_string_field(self):
        six.assertCountEqual(self, describe_field("user", "Bob"),
                             {"name": "user", "type": "string", "mode":
                              "nullable"})


class TestSchemaGenerator(unittest.TestCase):

    def test_simple_record(self):
        record = {"username": "Bob", "id": 123}
        schema = [{"name": "username", "type": "string", "mode": "nullable"},
                  {"name": "id", "type": "integer", "mode": "nullable"}]

        six.assertCountEqual(self, schema_from_record(record), schema)

    def test_hierarchical_record(self):
        record = {"user": {"username": "Bob", "id": 123}}
        schema = [{"name": "user", "type": "record", "mode": "nullable",
                   "fields": [{"name": "username", "type": "string", "mode":
                               "nullable"}, {"name": "id", "type": "integer",
                                             "mode": "nullable"}]}]
        generated_schema = schema_from_record(record)
        schema_fields = schema[0].pop('fields')
        generated_fields = generated_schema[0].pop('fields')
        six.assertCountEqual(self, schema_fields, generated_fields)
        six.assertCountEqual(self, generated_schema, schema)

    def test_hierarchical_record_with_timestamps(self):
        record = {"global": "2001-01-01", "user": {"local": "2001-01-01"}}

        schema_with_ts = [
            {"name": "global", "type": "timestamp", "mode": "nullable"},
            {"name": "user", "type": "record", "mode": "nullable",
                "fields": [{
                    "name": "local",
                    "type": "timestamp",
                    "mode": "nullable"}]}]

        schema_without_ts = [
            {"name": "global", "type": "string", "mode": "nullable"},
            {"name": "user", "type": "record", "mode": "nullable",
                "fields": [{
                    "name": "local",
                    "type": "string",
                    "mode": "nullable"}]}]

        six.assertCountEqual(self, schema_from_record(record), schema_with_ts)

        six.assertCountEqual(
            self, schema_from_record(record, timestamp_parser=lambda x: False),
            schema_without_ts)

    def test_repeated_field(self):
        record = {"ids": [1, 2, 3, 4, 5]}
        schema = [{"name": "ids", "type": "integer", "mode": "repeated"}]

        six.assertCountEqual(self, schema_from_record(record), schema)

    def test_nested_invalid_type_reported_correctly(self):
        key = "wrong answer"
        value = "wrong answer"

        try:
            schema_from_record({"a": {"b": [{"c": None}]}})
        except InvalidTypeException as e:
            key = e.key
            value = e.value

        self.assertEqual(key, "a.b.c")
        self.assertEqual(value, None)
