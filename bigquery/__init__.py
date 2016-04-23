from __future__ import absolute_import

from .version import __version__

from .client import get_client
from .client import (
    BIGQUERY_SCOPE,
    BIGQUERY_SCOPE_READ_ONLY,
    JOB_CREATE_IF_NEEDED,
    JOB_CREATE_NEVER,
    JOB_SOURCE_FORMAT_NEWLINE_DELIMITED_JSON,
    JOB_SOURCE_FORMAT_DATASTORE_BACKUP,
    JOB_SOURCE_FORMAT_CSV,
    JOB_WRITE_TRUNCATE,
    JOB_WRITE_APPEND,
    JOB_WRITE_EMPTY,
    JOB_ENCODING_UTF_8,
    JOB_ENCODING_ISO_8859_1
)

from .schema_builder import schema_from_record
