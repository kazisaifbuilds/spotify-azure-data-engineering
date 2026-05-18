import dlt
from pyspark.sql.functions import *

@dlt.table(name="dimtrack_stg")
def dimtrack_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimtrack")
    return df


dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = col("updated_at"),
  track_history_except_column_list = None,
  name=None,
  once=False,
  stored_as_scd_type = 2
)