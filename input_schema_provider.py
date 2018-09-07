#!/bin/python -u
from pyspark.sql import types as Types
from util.EtlLogger import EtlLogger

class InputSchemaProvider(object):
    def __init__(self, config):
        self.logger = EtlLogger.get_logger(self.__class__.__name__)
        schema_token = '_SCHEMA'
        self.schemas = {}
        for k, val in config.items():
            if k.endswith(schema_token):
                schema = [Types.StructField(itm[0].replace('.', '_'), Types.StringType(), False) for itm in val]
                key = k[:len(k)-len(schema_token)].lower()
                self.schemas[key] = Types.StructType(schema)
    def get_schema(self, schema_name):
        return self.schemas.get(schema_name)
