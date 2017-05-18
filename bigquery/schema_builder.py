from __future__ import absolute_import
__author__ = 'Aneil Mallavarapu (http://github.com/aneilbaboo)'

from datetime import datetime

import six
import dateutil.parser

from .errors import InvalidTypeException


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

    Parameters
    ----------
    record : dict
        Example of a record that is to be inserted into BigQuery
    timestamp_parser : function, optional
        Unary function taking a ``str`` and returning and ``bool`` that is
        True if the string represents a date

    Returns
    -------
    Schema: list
    """
    return [describe_field(k, v, timestamp_parser=timestamp_parser)
            for k, v in list(record.items())]


def describe_field(k, v, timestamp_parser=default_timestamp_parser):
    """Given a key representing a column name and value representing the value
    stored in the column, return a representation of the BigQuery schema
    element describing that field. Raise errors if invalid value types are
    provided.

    Parameters
    ----------
    k : Union[str, unicode]
        Key representing the column
    v : Union[str, unicode, int, float, datetime, object]
        Value mapped to by `k`

    Returns
    -------
    object
        Describing the field

    Raises
    ------
    Exception
        If invalid value types are provided.

    Examples
    --------
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
            field['fields'] = schema_from_record(v, timestamp_parser)
        except InvalidTypeException as e:
            # recursively construct the key causing the error
            raise InvalidTypeException("%s.%s" % (k, e.key), e.value)

    return field


def bigquery_type(o, timestamp_parser=default_timestamp_parser):
    """Given a value, return the matching BigQuery type of that value. Must be
    one of str/unicode/int/float/datetime/record, where record is a dict
    containing value which have matching BigQuery types.

    Parameters
    ----------
    o : object
        A Python object
    time_stamp_parser : function, optional
        Unary function taking a ``str`` and returning and ``bool`` that is
        True if the string represents a date

    Returns
    -------
    Union[str, None]
        Name of the corresponding BigQuery type for `o`, or None if no type
        could be found

    Examples
    --------
    >>> bigquery_type("abc")
    "string"
    >>> bigquery_type(123)
    "integer"
    """

    t = type(o)
    if t in six.integer_types:
        return "integer"
    elif (t == six.binary_type and six.PY2) or t == six.text_type:
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
