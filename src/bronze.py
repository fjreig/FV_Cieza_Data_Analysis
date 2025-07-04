import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
STORAGE_URI = "http://172.18.0.4:9000"      # Minio IP address from docker inspect

# Configure Spark with necessary packages and Iceberg/Nessie settings
conf = (
    pyspark.SparkConf()
        .setAppName('sales_data_app')
        # Include necessary packages
        .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
        # Enable Iceberg and Nessie extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        # Configure Nessie catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        # Set Minio as the S3 endpoint for Iceberg storage
        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
)

# Start Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Session Started")

## Postgres Config
url_read = "jdbc:postgresql://" + os.environ['POSTGRES_HOST'] + ":" + os.environ['POSTGRES_PORT'] + "/Monitorizacion"

## Querys
query_aarr = "select * from public.aarr where instalacion = 11 and equipo = 42"
query_inv = "select * from public.inversor where instalacion = 11"
query_emi = "select * from public.emi where instalacion = 11"
query_logger = "select * from public.logger where instalacion = 11"
query_variador = "select * from public.variador where instalacion = 11"
query_pcs = "select * from public.pcs where instalacion = 11"
query_rack = "select * from public.rack where instalacion = 11"
query_bateria = "select * from public.bateria where instalacion = 11"
query_prediccion_meteo = "select * from public.prediccion_meteo where localidad = 'cieza'"
query_omie = "select * from public.omie"

def Consultar_Postgres(New_query):
    df = (spark.read
    .format("jdbc")
    .option("url", url_read)
    .option("query", New_query)
    .option("driver", "org.postgresql.Driver")
    .option("user", os.environ['POSTGRES_PASSWORD'])
    .option("password", os.environ['POSTGRES_PASSWORD'])
    .load()
    )
    df.show()
    return(df)

def main():
    df_aarr = Consultar_Postgres(query_aarr)
    df_inv = Consultar_Postgres(query_inv)
    df_emi = Consultar_Postgres(query_emi)
    df_logger = Consultar_Postgres(query_logger)
    df_variador = Consultar_Postgres(query_variador)
    df_pcs = Consultar_Postgres(query_pcs)
    df_rack = Consultar_Postgres(query_rack)
    df_bateria = Consultar_Postgres(query_bateria)
    df_prediccion_meteo = Consultar_Postgres(query_prediccion_meteo)

    # Create the "monitorizacion" namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze;").show()

    # Write the DataFrame to an Iceberg table in the Nessie catalog
    df_aarr.writeTo("nessie.bronze.aarr").createOrReplace()
    df_inv.writeTo("nessie.bronze.inversor").createOrReplace()
    df_emi.writeTo("nessie.bronze.emi").createOrReplace()
    df_logger.writeTo("nessie.bronze.logger").createOrReplace()
    df_variador.writeTo("nessie.bronze.variador").createOrReplace()
    df_pcs.writeTo("nessie.bronze.pcs").createOrReplace()
    df_rack.writeTo("nessie.bronze.rack").createOrReplace()
    df_bateria.writeTo("nessie.bronze.bateria").createOrReplace()
    df_prediccion_meteo.writeTo("nessie.bronze.prediccion_meteo").createOrReplace()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()