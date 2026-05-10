from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json, date_format,
    trim, broadcast, input_file_name, regexp_extract, concat, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import argparse
import time
import traceback
import pytz

warehouse_path = "s3://company-datalake/"
CLEAN_DATABASE_NAME = "glue_catalog.silver"

app_name = "raw_to_clean"

