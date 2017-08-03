BigQuery-Python
===============
<a href="https://travis-ci.org/tylertreat/BigQuery-Python"><img align="right" src="https://travis-ci.org/tylertreat/BigQuery-Python.svg"></a>
Simple Python client for interacting with Google BigQuery.

This client provides an API for retrieving and inserting BigQuery data by wrapping Google's low-level API client library. It also provides facilities that make it convenient to access data that is tied to an App Engine appspot, such as request logs.

[Documentation](http://tylertreat.github.io/BigQuery-Python/)

# Installation

`pip install bigquery-python`

# Basic Usage

```python
from bigquery import get_client

# BigQuery project id as listed in the Google Developers Console.
project_id = 'project_id'

# Service account email address as listed in the Google Developers Console.
service_account = 'my_id_123@developer.gserviceaccount.com'

# PKCS12 or PEM key provided by Google.
key = 'key.pem'

client = get_client(project_id, service_account=service_account,
                    private_key_file=key, readonly=True)

# JSON key provided by Google
json_key = 'key.json'
 
client = get_client(json_key_file=json_key, readonly=True)

# Submit an async query.
job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')

# Check if the query has finished running.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
results = client.get_query_rows(job_id)
```

# Executing Queries

The BigQuery client allows you to execute raw queries against a dataset. The `query` method inserts a query job into BigQuery. By default, `query` method runs asynchronously with `0` for `timeout`. When a non-zero timeout value is specified, the job will wait for the results, and throws an exception on timeout.

When you run an async query, you can use the returned `job_id` to poll for job status later with `check_job`.

```python
# Submit an async query
job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')

# Do other stuffs

# Poll for query completion.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
if complete:
    results = client.get_query_rows(job_id)
```

You can also specify a non-zero timeout value if you want your query to be synchronous.

```python
# Submit a synchronous query
try:
    _job_id, results = client.query('SELECT * FROM dataset.my_table LIMIT 1000', timeout=10)
except BigQueryTimeoutException:
    print "Timeout"
```

## Query Builder

The `query_builder` module provides an API for generating query strings that can be run using the BigQuery client.

```python
from bigquery.query_builder import render_query

selects = {
    'start_time': {
        'alias': 'Timestamp',
        'format': 'INTEGER-FORMAT_UTC_USEC'
    }
}

conditions = [
    {
        'field': 'Timestamp',
        'type': 'INTEGER',
        'comparators': [
            {
                'condition': '>=',
                'negate': False,
                'value': 1399478981
            }
        ]
    }
]

grouping = ['Timestamp']

having = [
    {
        'field': 'Timestamp',
        'type': 'INTEGER',
        'comparators': [
            {
                'condition': '==',
                'negate': False,
                'value': 1399478981
            }
        ]
    }
]

order_by ={'fields': ['Timestamp'], 'direction': 'desc'}

query = render_query(
    'dataset',
    ['table'],
    select=selects,
    conditions=conditions,
    groupings=grouping,
    having=having,
    order_by=order_by,
    limit=47
)

job_id, _ = client.query(query)
```

# Managing Tables

The BigQuery client provides facilities to manage dataset tables, including creating, deleting, checking the existence, and getting the metadata of tables.

```python
# Create a new table.
schema = [
    {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
    {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
]
created = client.create_table('dataset', 'my_table', schema)

# Delete an existing table.
deleted = client.delete_table('dataset', 'my_table')

# Check if a table exists.
exists = client.check_table('dataset', 'my_table')

# Get a table's full metadata. Includes numRows, numBytes, etc. 
# See: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
metadata = client.get_table('dataset', 'my_table')
```

There is also functionality for retrieving tables that are associated with a Google App Engine appspot, assuming table names are in the form of appid_YYYY_MM or YYYY_MM_appid. This allows tables between a date range to be selected and queried on.

```python
# Get appspot tables falling within a start and end time.
from datetime import datetime, timedelta
range_end = datetime.utcnow()
range_start = range_end - timedelta(weeks=12)
tables = client.get_tables('dataset', 'appid', range_start, range_end)
```

# Inserting Data

The client provides an API for inserting data into a BigQuery table. The last parameter refers to an optional insert id key used to avoid duplicate entries.

```python
# Insert data into table.
rows =  [
    {'one': 'ein', 'two': 'zwei'}
    {'id': 'NzAzYmRiY', 'one': 'uno', 'two': 'dos'},
    {'id': 'NzAzYmRiY', 'one': 'ein', 'two': 'zwei'} # duplicate entry
]

inserted = client.push_rows('dataset', 'table', rows, 'id')
```

# Write Query Results to Table
You can write query results directly to table. When either dataset or table parameter is omitted, query result will be written to temporary table.
```python
# write to permanent table
job = client.write_to_table('SELECT * FROM dataset.original_table LIMIT 100',
                            'dataset',
                            'table')
try:
    job_resource = client.wait_for_job(job, timeout=60)
    print job_resource
except BigQueryTimeoutException:
    print "Timeout"

# write to permanent table with UDF in query string
external_udf_uris = ["gs://bigquery-sandbox-udf/url_decode.js"]
query = """SELECT requests, title
            FROM
              urlDecode(
                SELECT
                  title, sum(requests) AS num_requests
                FROM
                  [fh-bigquery:wikipedia.pagecounts_201504]
                WHERE language = 'fr'
                GROUP EACH BY title
              )
            WHERE title LIKE '%ç%'
            ORDER BY requests DESC
            LIMIT 100
        """
job = client.write_to_table(
  query,
  'dataset',
  'table',
  external_udf_uris=external_udf_uris
)

try:
    job_resource = client.wait_for_job(job, timeout=60)
    print job_resource
except BigQueryTimeoutException:
    print "Timeout"

# write to temporary table
job = client.write_to_table('SELECT * FROM dataset.original_table LIMIT 100')
try:
    job_resource = client.wait_for_job(job, timeout=60)
    print job_resource
except BigQueryTimeoutException:
    print "Timeout"


```

# Import data from Google cloud storage
```python
schema = [ {"name": "username", "type": "string", "mode": "nullable"} ]
job = client.import_data_from_uris( ['gs://mybucket/mydata.json'],
                                    'dataset',
                                    'table',
                                    schema,
                                    source_format=JOB_SOURCE_FORMAT_JSON)

try:
    job_resource = client.wait_for_job(job, timeout=60)
    print job_resource
except BigQueryTimeoutException:
    print "Timeout"
```

# Export data to Google cloud storage
```python
job = client.export_data_to_uris( ['gs://mybucket/mydata.json'],
                                   'dataset',
                                   'table')
try:
    job_resource = client.wait_for_job(job, timeout=60)
    print job_resource
except BigQueryTimeoutException:
    print "Timeout"
```

# Managing Datasets

The client provides an API for listing, creating, deleting, updating and patching datasets.

```python
# List datasets
datasets = client.get_datasets()


# Create dataset
dataset = client.create_dataset('mydataset', friendly_name="My Dataset", description="A dataset created by me")

# Get dataset
client.get_dataset('mydataset')

# Delete dataset
client.delete_dataset('mydataset')
client.delete_dataset('mydataset', delete_contents=True) # delete even if it contains data

# Update dataset
client.update_dataset('mydataset', friendly_name="mon Dataset") # description is deleted

# Patch dataset
client.patch_dataset('mydataset', friendly_name="mon Dataset") # friendly_name changed; description is preserved

# Check if dataset exists.
exists = client.check_dataset('mydataset')
```

# Creating a schema from a sample record
```python
from bigquery import schema_from_record

schema_from_record({"id":123, "posts": [{"id":123, "text": "this is a post"}], "username": "bob"})
```

# Contributing

Requirements to commit here:

  - Branch off master, PR back to master.
  - Your code should pass [Flake8](http://flake8.readthedocs.org/en/latest/).
  - Unit test coverage is required.
  - Good docstrs are required.
  - Good [commit messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) are required.
