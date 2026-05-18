import dlt
from pyspark.sql.functions import *

@dlt.table(name="factstream_stg")
def factstream_stg():
    df = spark.readStream.table("spotify_catalog.silver.factstream")
    return df


dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = col("stream_timestamp"),
  track_history_except_column_list = None,
  name=None,
  once=False,
  stored_as_scd_type = 1
)