class JSONSchemaError(Exception):
    """
    Raise this when there is an error with regards to an instance of JSON Schema
    """


class TargetError(Exception):
    """
    Raise when there is an Exception streaming data to the target.
    """


class PostgresError(Exception):
    """
    Raise this when there is an error with regards to Postgres streaming
    """


class SingerStreamError(Exception):
    """
    Raise when there is an Exception with Singer Streams.
    """
