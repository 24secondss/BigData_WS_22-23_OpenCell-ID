FROM marcelmittelstaedt/airflow:latest
COPY ./mariadb-java-client-3.1.0.jar /home/airflow/spark/jars
COPY --chown=airflow ./airflow/ /home/airflow/airflow
RUN docker exec -it airflow bash && sudo su airflow && cd && airflow unpause OpenCelliD_full_db
