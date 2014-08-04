__author__ = 'Aneil Mallavarapu (http://github.com/aneilbaboo)'

import dateutil.parser
from datetime import datetime

def default_timestamp_parser(s):
    try:
        if dateutil.parser.parse(s):
            return True
        else:
            return False
    except:
        return False

def schema_from_record(record,timestamp_parser=default_timestamp_parser):
    """Generates a BigQuery schema given an example of a record that is to be inserted into BigQuery
    Args:
        record: dict
        timestamp_parser: unary function taking a string and return non-NIL if string represents a date
    Returns:
        schema: list"""
    return [describe_field(k,v) for k,v in record.items()]

def describe_field(k,v,timestamp_parser=default_timestamp_parser):
    """Given a key representing a column name and value representing the value stored in the column,
    returns a representation of the BigQuery schema element describing that field.
    Raises errors if invalid value types are provided.
    Args:
        k: str/unicode, key representing the column
        v: str/unicode/int/float/datetime/object
    Returns:
        object describing the field

    >>> describe_field("username","Bob")
    {"name":"username", "type":"string", "mode":"nullable"}
    >>> describe_field("users",[{"username":"Bob"}])
    {"name":"users", "type":"record", "mode":"repeated", fields:[{"name":"username","type":"string","mode":"nullable"}]}
    """
    def bq_schema_field(name, type, mode):
        return {"name":name,"type":type,"mode":mode}

    if isinstance(v,list):
        if len(v)==0:
            raise Exception("Can't describe schema because of empty list {0}:[]".format(k))
        v = v[0]
        mode = "repeated"
    else:
        mode = "nullable"

    bq_type = bigquery_type(v,timestamp_parser=timestamp_parser)
    field = bq_schema_field(k,bq_type,mode)
    if bq_type=="record":
        field['fields'] = schema_from_record(v)

    return field

def bigquery_type(o,timestamp_parser=default_timestamp_parser):
    """Given a value, returns the matching BigQuery type of that value.
    Must be one of str/unicode/int/float/datetime/record,
    where record is a dict containing value which have matching BigQuery types.

    >>> bigquery_type("abc")
    "string"
    >>> bigquery_type(123)
    "integer"
    """

    t = type(o)
    if t==int:
        return "integer"
    elif t==str or t==unicode:
        if timestamp_parser and timestamp_parser(o):
            return "timestamp"
        else:
            return "string"
    elif t==float:
        return "float"
    elif t==bool:
        return "boolean"
    elif t==dict:
        return "record"
    elif t==datetime:
        return "timestamp"
    else:
        raise Exception("Invalid object for BigQuery schema: {0} ({1}".format(o,t))

