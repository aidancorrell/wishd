"""A Spark job shaped like a dbt model materialisation.

Deliberately mirrors what dbt-spark does on EMR: read two source tables, join
them, write the result. That is the shape whose OpenLineage output we need to
verify -- specifically the job naming and whether the parent facet's `root`
block is trustworthy (openlineage-dbt's was not).
"""

import os
from pyspark.sql import SparkSession

WAREHOUSE = "/tmp/warehouse"

spark = (
    SparkSession.builder.appName("dbt_spark_analytics")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .enableHiveSupport()
    .getOrCreate()
)

# Tag the run the way an EMR bootstrap action would, so events carry identity.
spark.sparkContext.setLocalProperty("spark.datadog.tags.model", "fct_orders")

orders = spark.createDataFrame(
    [(1, 10, 25.0), (2, 11, 40.0), (3, 10, 15.5)],
    "order_id int, customer_id int, order_total double",
)
customers = spark.createDataFrame(
    [(10, "enterprise"), (11, "smb")], "customer_id int, segment string"
)

orders.write.mode("overwrite").saveAsTable("stg_orders")
customers.write.mode("overwrite").saveAsTable("stg_customers")

fct = spark.sql("""
    select o.order_id, o.customer_id, c.segment, o.order_total
    from stg_orders o
    join stg_customers c on c.customer_id = o.customer_id
""")
fct.write.mode("overwrite").saveAsTable("fct_orders")

print("[dataspine] rows written:", spark.table("fct_orders").count())
spark.stop()
