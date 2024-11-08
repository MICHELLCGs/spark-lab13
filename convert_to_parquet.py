from pyspark.sql import SparkSession
import os
import requests

def download_file(url, local_path):
    """Descargar un archivo desde una URL y guardarlo localmente."""
    try:
        print(f"Descargando {url}...")
        response = requests.get(url)
        response.raise_for_status()  # Lanza un error si la descarga falla
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print("Descarga completada.")
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")

def main():
    # Crear la sesión de Spark
    spark = SparkSession.builder \
        .appName("MovieLensCSVtoParquet") \
        .getOrCreate()

    # Rutas de los archivos
    movies_csv_path = "/app/movies.csv"  # Cambia esto a la ruta de tu archivo
    movies_csv_url = "https://tecmovielens.s3.us-east-1.amazonaws.com/ml-latest/movies.csv"

    # Verificar si el archivo CSV existe, si no, descargarlo
    if not os.path.exists(movies_csv_path):
        download_file(movies_csv_url, movies_csv_path)

    # Leer el archivo de películas
    print("Leyendo el archivo de películas...")
    movies_df = spark.read.csv(movies_csv_path, header=True, inferSchema=True)

    # Escribir en formato Parquet
    print("Convirtiendo y guardando películas en formato Parquet...")
    movies_df.write.parquet("/app/movies_parquet")  # Cambia esto a la ruta donde deseas guardar el Parquet

    print("Conversión completada.")

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
