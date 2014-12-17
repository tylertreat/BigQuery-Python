class UnfinishedQueryException(Exception):
    pass


class BigQueryTimeoutException(Exception):
    pass


class JobInsertException(Exception):
    pass


class JobExecutingException(Exception):
    pass


class InvalidTypeException(Exception):

    def __init__(self, k, v):
        self.key = k
        self.value = v

        message = "Invalid type at key '{key}': {value}".format(
            value=v, key=k)
        Exception.__init__(self, message)
