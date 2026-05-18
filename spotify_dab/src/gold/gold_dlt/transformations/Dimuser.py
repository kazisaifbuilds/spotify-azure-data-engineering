import dlt
from pyspark.sql.functions import *

expectations ={
  "rule_1":"user_id IS NOT NULL"
}

@dlt.expect_all_or_drop(expectations)
@dlt.table(name="dimuser_stg")
def dimuser_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimuser")
    return df


dlt.create_streaming_table(
  name = "dimuser",
  expect_all_or_drop=expectations)

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = col("updated_at"),
  track_history_except_column_list = None,
  name=None,
  once=False,
  stored_as_scd_type = 2
)