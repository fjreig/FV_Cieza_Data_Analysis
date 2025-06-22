# FV_Cieza_Data_Analysis
Data tec

```
docker inspect minio
```

## Spark

docker exec -it spark pip install py4j

```
docker exec -it spark python3 /opt/spark-apps/bronze.py
docker exec -it spark python3 /opt/spark-apps/silver.py
docker exec -it spark python3 /opt/spark-apps/gold.py
```

## Nessie

```
http://localhost:19120/tree/main
```

## Minio

```
http://localhost:9001/login
```