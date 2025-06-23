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
    # Create the "monitorizacion" namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver;").show()

    # Verify by reading from the Iceberg table
    df_aarr = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            round(avg(pa),1) as pa,
            round(max(ea_import)-min(ea_import),1) as ea_import,
            round(max(ea_export)-min(ea_export),1) as ea_export
        FROM
            nessie.bronze.aarr 
        GROUP BY
            time_interval
        ORDER BY
            time_interval 
        """
        )
    df_aarr.writeTo("nessie.silver.aarr").createOrReplace()
    
    df_inversores = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa),1) as pa, round(avg(regulacion),1) as regulacion,
            round(max(ea_diaria)-min(ea_diaria),1) as ea_gen
        FROM
            nessie.bronze.inversor 
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        )
    df_inversores.writeTo("nessie.silver.inversor").createOrReplace()
    
    df_logger = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            round(avg(pa),1) as pa_gen, round(avg(pa_bat),1) as pa_bat,
            round(max(ea_diaria)-min(ea_diaria),1) as ea_gen
        FROM
            nessie.bronze.logger 
        GROUP BY
            time_interval
        ORDER BY
            time_interval
        """
        )
    df_logger.writeTo("nessie.silver.logger").createOrReplace()
    
    df_variadores = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa_motor),1) as pa, round(avg(frec_motor),1) as frecuencia
        FROM
            nessie.bronze.variador 
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        )
    df_variadores.writeTo("nessie.silver.variador").createOrReplace()
    
    df_pcs = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa),1) as pa,
            round(max(ea_import)-min(ea_import),1) as ea_import,
            round(max(ea_export)-min(ea_export),1) as ea_export
        FROM
            nessie.bronze.pcs 
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        )
    df_pcs.writeTo("nessie.silver.pcs").createOrReplace()
    
    df_bateria = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            equipo, round(avg(pa),1) as pa, round(avg(soc),1) as soc,
            round(max(ea_import_hoy)-min(ea_import_hoy),1) as ea_carga,
            round(max(ea_export_hoy)-min(ea_export_hoy),1) as ea_descarga
        FROM
            nessie.bronze.bateria
        WHERE 
            equipo = 98 or equipo = 99
        GROUP BY
            time_interval, equipo
        ORDER BY
            time_interval, equipo
        """
        )
    df_bateria.writeTo("nessie.silver.bateria").createOrReplace()
    
    df_predicciones_meteo = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) as time_interval,
            round(avg(radiacion),1) as radiacion, round(avg(temperatura),1) as temperatura
        FROM
            nessie.bronze.prediccion_meteo 
        GROUP BY
            time_interval
        ORDER BY
            time_interval
        """
        )
    df_predicciones_meteo.writeTo("nessie.silver.prediccion_meteo").createOrReplace()

    df_emi = spark.sql(
        """
        SELECT *
        FROM nessie.bronze.emi
        """
        )
    df_emi_explode = df_emi.select("fecha", json_tuple(col("radiacion"), "radiacion", "radiacion1", "radiacion2")) \
             .toDF("fecha", "radiacion", "radiacion1", "radiacion2")
    df_emi_explode.createOrReplaceTempView("emi_explode")
    df_emi_explode = spark.sql(
        """
        SELECT
            date_trunc('hour', fecha) + interval '15 minutes' * floor(date_part('minute', fecha) / 15) as time_interval,
            round(avg(radiacion),1) as radiacion
        FROM
            emi_explode 
        GROUP BY
            time_interval
        ORDER BY
            time_interval
        """
        )
    df_emi_explode.writeTo("nessie.silver.emi").createOrReplace()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()