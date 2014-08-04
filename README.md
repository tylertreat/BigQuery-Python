BigQuery-Python
===============
<a href="https://travis-ci.org/tylertreat/BigQuery-Python"><img align="right" src="https://travis-ci.org/tylertreat/BigQuery-Python.svg"></a>
Simple Python client for interacting with Google BigQuery.

This client provides an API for retrieving and inserting BigQuery data by wrapping Google's low-level API client library. It also provides facilities that make it convenient to access data that is tied to an App Engine appspot, such as request logs.

# Basic Usage #

```python
from bigquery.client import get_client

# BigQuery project id as listed in the Google Developers Console.
project_id = 'project_id'

# Service account email address as listed in the Google Developers Console.
service_account = 'my_id_123@developer.gserviceaccount.com'

# PKCS12 or PEM key provided by Google.
key = 'secret_key'

client = get_client(project_id, service_account=service_account, private_key=key, readonly=True)

# Submit a query.
job_id, results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')

# Check if the query has finished running.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
results = client.get_query_rows(job_id)
```

# Executing Queries #

The BigQuery client allows you to execute raw queries against a dataset. The `query` method inserts a query job into BigQuery. A timeout can be specified to wait for the results, after which the request will return and can later be polled with `check_job`. This timeout defaults to 10 seconds.

```python
# Submit query and wait 5 seconds for results.
job_id, results = client.query('SELECT * FROM dataset.my_table LIMIT 1000', timeout=5)
```

If results are not available before the timeout expires, the job id can be used to poll for them.

```python
# Poll for query completion.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
if complete:
    results = client.get_query_rows(job_id)
```

## Query Builder ##

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

query = render_query(
    'dataset',
    ['table'],
    select=selects,
    conditions=conditions,
    groupings=['Timestamp'],
    order_by={'field': 'Timestamp', 'direction': 'desc'}
)

job_id, _ = client.query(query, timeout=0)
```

# Managing Tables

The BigQuery client provides facilities to manage dataset tables, including creating, deleting, and checking the existence of tables.

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

# Managing datasets

The client provides an API for listing, creating, deleting, updating and patching datasets.

```python
# List datasets
datasets = client.get_datasets()


# Create dataset
dataset = client.create_dataset('mydataset',friendly_name="My Dataset",description="A dataset created by me")

# Get dataset
client.get_dataset('mydataset')

# Delete dataset
client.delete_dataset('mydataset')

# Update dataset
client.update_dataset('mydataset',friendly_name="mon Dataset") # description is deleted

# Patch dataset
client.patch_dataset('mydataset',friendly_name="mon Dataset") # friendly_name changed; description is preserved
```

# Creating a schema from a sample record
```python
client.schema_from_record({id:123, posts:[ {id:123, text:"tihs is a post"} ], username: "bob"})
```

# Caveats

BigQuery [flattens](https://developers.google.com/bigquery/docs/data?hl=ja#flatten) results with repeated records, so a result might actually map to multiple rows. This means that the row count may be larger than the actual number of results because BigQuery reports the number of unrolled rows but the returned results are rolled back up.
