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

docker exec -it spark python3 /opt/spark-apps/Tabla_conjunto.py
```

## Nessie

```
http://localhost:19120/tree/main
```

## Minio

```
http://localhost:9001/login
```

## Superset

```
docker exec -it superset pip install --upgrade pip
docker exec -it superset pip install sqlalchemy_dremio
```

```
docker exec -it superset superset fab create-admin \
              --username admin \
              --firstname Admin \
              --lastname Piae \
              --email correo@gmail.com \
              --password admin01
```

```
docker exec -it superset superset db upgrade
```

```
docker exec -it superset superset init
```

```
dremio+flight://pavener:javi$4875@dremio:32010/?UseEncryption=false
dremio+pyodbc://pavener:javi$4875@dremio:31010
```

### 5.1 Configurar Catalogo Nessie
General settings tab
* Source Name: nessie
* Nessie Endpoint URL: http://nessie:19120/api/v2
* Auth Type: None

Storage settings tab
* AWS Root Path: warehouse
* AWS Access Key: admin
* AWS Secret Key: password
* Uncheck “Encrypt Connection” Box (since we aren’t using SSL)
* Connection Properties

| Key | Value |
| ------------- |:-------------:|
| fs.s3a.path.style.access | true |
| fs.s3a.endpoint | minio:9000 |
| dremio.s3.compat | true |
