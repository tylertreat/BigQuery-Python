__author__ = 'Aneil Mallavarapu (http://github.com/aneilbaboo)'

from datetime import datetime

import dateutil.parser

from errors import InvalidTypeException


def default_timestamp_parser(s):
    try:
        if dateutil.parser.parse(s):
            return True
        else:
            return False
    except:
        return False


def schema_from_record(record, timestamp_parser=default_timestamp_parser):
    """Generate a BigQuery schema given an example of a record that is to be
    inserted into BigQuery.

    Args:
        record: dict
        timestamp_parser: unary function taking a string and return non-NIL if
                          string represents a date

    Returns:
        schema: list
    """
    return [describe_field(k, v, timestamp_parser=timestamp_parser)
            for k, v in record.items()]


def describe_field(k, v, timestamp_parser=default_timestamp_parser):
    """Given a key representing a column name and value representing the value
    stored in the column, return a representation of the BigQuery schema
    element describing that field. Raise errors if invalid value types are
    provided.

    Args:
        k: str/unicode, key representing the column
        v: str/unicode/int/float/datetime/object

    Returns:
        object describing the field

    Raises:
        Exception: if invalid value types are provided.

    >>> describe_field("username", "Bob")
    {"name": "username", "type": "string", "mode": "nullable"}
    >>> describe_field("users", [{"username": "Bob"}])
    {"name": "users", "type": "record", "mode": "repeated",
     "fields": [{"name":"username","type":"string","mode":"nullable"}]}
    """

    def bq_schema_field(name, bq_type, mode):
        return {"name": name, "type": bq_type, "mode": mode}

    if isinstance(v, list):
        if len(v) == 0:
            raise Exception(
                "Can't describe schema because of empty list {0}:[]".format(k))
        v = v[0]
        mode = "repeated"
    else:
        mode = "nullable"

    bq_type = bigquery_type(v, timestamp_parser=timestamp_parser)
    if not bq_type:
        raise InvalidTypeException(k, v)

    field = bq_schema_field(k, bq_type, mode)
    if bq_type == "record":
        try:
            field['fields'] = schema_from_record(v)
        except InvalidTypeException, e:
            # recursively construct the key causing the error
            raise InvalidTypeException("%s.%s" % (k, e.key), e.value)

    return field


def bigquery_type(o, timestamp_parser=default_timestamp_parser):
    """Given a value, return the matching BigQuery type of that value. Must be
    one of str/unicode/int/float/datetime/record, where record is a dict
    containing value which have matching BigQuery types.

    Returns:
        str or None if no matching type could be found

    >>> bigquery_type("abc")
    "string"
    >>> bigquery_type(123)
    "integer"
    """

    t = type(o)
    if t == int:
        return "integer"
    elif t == str or t == unicode:
        if timestamp_parser and timestamp_parser(o):
            return "timestamp"
        else:
            return "string"
    elif t == float:
        return "float"
    elif t == bool:
        return "boolean"
    elif t == dict:
        return "record"
    elif t == datetime:
        return "timestamp"
    else:
        return None  # failed to find a type
