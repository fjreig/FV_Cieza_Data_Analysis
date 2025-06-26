import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import json_tuple, col
import os

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
STORAGE_URI = "http://172.18.0.3:9000"      # Minio IP address from docker inspect

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

def main():
    # Verify by reading from the Iceberg table
    spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa),1) as pa, 
            round(max(ea_import)-min(ea_import),1) as ea_carga,
            round(max(ea_export)-min(ea_export),1) as ea_descarga
        FROM
            nessie.bronze.pcs
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        ).show()
    
    spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa),1) as pa, round(avg(soc),1) as soc, round(avg(soh),1) as soh, round(avg(dod),1) as dod, 
            round(avg(capacidad_carga),1) as capacidad_carga, round(avg(capacidad_descarga),1) as capacidad_descarga, 
            round(max(ea_carga_hoy)-min(ea_carga_hoy),1) as ea_carga,
            round(max(ea_descarga_hoy)-min(ea_descarga_hoy),1) as ea_descarga
        FROM
            nessie.bronze.rack
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        ).show()
 
  
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()