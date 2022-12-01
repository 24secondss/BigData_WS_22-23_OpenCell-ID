FROM marcelmittelstaedt/airflow:latest
COPY ./mariadb-java-client-3.1.0.jar /home/airflow/spark/jars
COPY --chown=airflow ./airflow/ /home/airflow/airflow
RUN docker compose exec -it airflow && sudo su airflow && cd && airflow unpause OpenCelliD_full_db
