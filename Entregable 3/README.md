Para ejecutar el DAG weather_ETL_DAG y levantar airflow debemos hacer lo siguiente:

1) Movernos a la carpeta Entregable 3
2) Ingresar nuestras credenciales propias para Redshift y nuestro api key de weather api
3) Ejecutar en la terminal el comando "docker-compose up -d" 
4) Ir una pestaña en el navegador e ingresar a http://localhost:8080/
5) Ingresar en la interfaz de airflow las credenciales para user: airflow y para password: airflow

Una vez dentrod e la interfaz debería visualizarse el Dag "weather_ETL_DAG".
