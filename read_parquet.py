from pyspark.sql import SparkSession
import time

def main():
    # Crear la sesión de Spark
    spark = SparkSession.builder \
        .appName("MovieLensReadParquet") \
        .getOrCreate()

    # Iniciar temporizador
    start_time = time.time()

    # Leer el archivo Parquet de películas
    print("Leyendo el archivo Parquet de películas...")
    movies_df = spark.read.parquet("/app/movies_parquet")  # Cambia esto a la ruta donde guardaste el Parquet

    # Mostrar el contenido del DataFrame de películas
    print("Contenido del DataFrame de películas:")
    movies_df.show()

    # Calcular el tiempo total de ejecución
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Formatear y mostrar el tiempo de ejecución
    print("|" + "-" * 67 + "|")
    print(f"| Tiempo total de ejecución: {elapsed_time:.2f} segundos |")
    print("|" + "-" * 67 + "|")

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
