BigQuery-Python
===============

Simple Python client for interacting with Google BigQuery.

This client provides an API for retrieving BigQuery data by wrapping Google's low-level API client library. It also provides facilities that make it convenient to access data that is tied to an App Engine appspot, such as request logs.

# Usage #

```python
from bigquery.client import get_client

# BigQuery project id as listed in the Google Developers Console.
project_id = 'project_id'

# Service account email address as listed in Google Developers Console.
service_account = 'my_id_123@developer.gserviceaccount.com'

# PKCS12 or PEM key provided by Google.
key = 'secret_key'

client = get_client(project_id, service_account=service_account, private_key=key)

# Submit a query.
job_id = client.query('SELECT * FROM my_table LIMIT 1000')

# Check if the query has finished running.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
results = client.get_query_rows(job_id)
```
