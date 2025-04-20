import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define Nessie and Minio configuration
CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3://warehouse/"
STORAGE_URI = "http://minio:9000"  # Or use Minio's container IP if DNS fails

conf = (
    pyspark.SparkConf()
    .setAppName('sales_data_app')
    .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1')
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
    .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
    .set('spark.sql.catalog.nessie.ref', 'main')
    .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
    .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
    .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
    .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
    .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Define schema matching your CSV
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date", StringType(), True)
])

# Load the CSV as a DataFrame
df = spark.read.csv("/data/salesdata.csv", header=True, schema=schema)

# Create namespace and table if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales")
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.sales.sales_data (
        id STRING,
        name STRING,
        product STRING,
        price DOUBLE,
        date STRING
    ) USING iceberg
""")

# Insert data into the Iceberg table
df.writeTo("nessie.sales.sales_data").append()

# Query and show some data
spark.read.table("nessie.sales.sales_data").show(5)

spark.stop()
