import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import to_date



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


#  Read JSON safely

#df = spark.read \
#    .option("multiline", "true") \
#   .json("s3://rithish-ecommerce-data-pipeline-2026/raw/orders/")
    


# Read using Glue (supports bookmark)
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://rithish-ecommerce-data-pipeline-2026/raw/orders/"],
        "recurse": True
    },
    format="json"
)

df = datasource.toDF()


#  Helper for safe columns

def safe_col(df, col_name):
    return col(col_name) if col_name in df.columns else lit(None)


#  Handle schema variations

df = df.withColumn(
    "user_id",
    when(safe_col(df, "user_id").isNotNull(), safe_col(df, "user_id"))
    .otherwise(safe_col(df, "userID"))
)

df = df.withColumn(
    "product_id",
    when(safe_col(df, "product_id").isNotNull(), safe_col(df, "product_id"))
    .otherwise(safe_col(df, "productID"))
)

df = df.withColumn(
    "order_timestamp",
    when(safe_col(df, "order_timestamp").isNotNull(), safe_col(df, "order_timestamp"))
    .otherwise(safe_col(df, "order_time"))
)


#  Handle null values

df = df.fillna({
    "category": "Unknown",
    "payment_method": "Unknown",
    "status": "Unknown"
})


#  Fix data types



#  Convert entire struct to string
df = df.withColumn("price_str", col("price").cast("string"))

# Extract numeric value using regex
df = df.withColumn(
    "price",
    regexp_extract(col("price_str"), r"(\d+\.?\d*)", 1).cast("double")
)

#  Same for quantity
df = df.withColumn("quantity_str", col("quantity").cast("string"))

df = df.withColumn(
    "quantity",
    regexp_extract(col("quantity_str"), r"(\d+)", 1).cast("int")
)

# Drop temp columns
df = df.drop("price_str", "quantity_str")

# Replace original columns
df = df.drop("price", "quantity")

df = df.withColumnRenamed("price_clean", "price")
df = df.withColumnRenamed("quantity_clean", "quantity")

#  Remove invalid records

df = df.filter(col("price").isNotNull())
df = df.filter(col("quantity").isNotNull())

#  Add processing column

df = df.withColumn("processed_time", current_timestamp())


# Write to S3

df.write.mode("overwrite").parquet(
    "s3://rithish-ecommerce-data-pipeline-2026/processed/orders/"
)

print("ETL Job Completed Successfully")