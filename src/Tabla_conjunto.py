import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import json_tuple, col
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

def main():
    # Verify by reading from the Iceberg table
    df_tabla_conjunto = spark.sql(
        """
        SELECT 
            nessie.gold.aarr.fecha, nessie.gold.aarr.ea_import, nessie.gold.aarr.ea_export,
            nessie.gold.bateria.ea_carga, nessie.gold.bateria.ea_descarga,
            nessie.gold.logger.ea_gen,
            round(nessie.gold.aarr.ea_import + nessie.gold.bateria.ea_descarga + nessie.gold.logger.ea_gen - 
                nessie.gold.aarr.ea_export - nessie.gold.bateria.ea_carga, 1) as ea_consumo,
            nessie.gold.emi.radiacion,
            nessie.gold.prediccion_meteo.radiacion as rad_pred
        FROM 
            nessie.gold.aarr
        join 
            nessie.gold.bateria on nessie.gold.bateria.fecha = nessie.gold.aarr.fecha
        join 
            nessie.gold.logger on nessie.gold.logger.fecha = nessie.gold.aarr.fecha
        join 
            nessie.gold.emi on nessie.gold.emi.fecha = nessie.gold.aarr.fecha
        join 
            nessie.gold.prediccion_meteo on nessie.gold.prediccion_meteo.fecha = nessie.gold.aarr.fecha
        """
        )
    
    df_tabla_conjunto.writeTo("nessie.gold.conjunto").createOrReplace()
  
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()