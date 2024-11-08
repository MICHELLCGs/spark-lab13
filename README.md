# Spark Stack Deployment LAB13

Este proyecto utiliza Docker para implementar un clúster de Spark. A continuación se detallan los comandos necesarios para desplegar, ejecutar y eliminar la pila.

## Comandos de Implementación

### Desplegar la Pila de Docker
Utiliza el siguiente comando para desplegar el clúster de Spark y Hadoop:

```bash
docker swarm init
```

```bash
docker stack deploy -c stack.yml spark_stack
```

### Eliminar la Pila de Docker (opcional)
Para eliminar la pila que acabas de desplegar, ejecuta:
```bash
docker stack rm spark_stack
```

## Instalación de Dependencias
Después de desplegar la pila, puedes instalar las dependencias necesarias especificadas en `requirements.txt` utilizando el siguiente comando:

```bash
docker exec -it $(docker ps --filter "name=spark-master" -q) pip install -r /app/requirements.txt
```

## Ejecución de Scripts de Spark

### Convertir CSV a Parquet
Para ejecutar el script que convierte un archivo CSV a formato Parquet, utiliza el siguiente comando:

```bash
docker exec -it $(docker ps --filter "name=spark-master" -q) /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /app/convert_to_parquet.py
```


### Leer el Archivo Parquet
Para leer el archivo Parquet generado y mostrar su contenido, ejecuta:

```bash
docker exec -it $(docker ps --filter "name=spark-master" -q) /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /app/read_parquet.py
```
