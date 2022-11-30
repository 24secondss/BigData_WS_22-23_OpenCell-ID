FROM marcelmittelstaedt/airflow:latest
COPY ./mariadb-java-client-3.1.0.jar /home/airflow/spark/jars
COPY --chown=airflow ./airflow/airflow/ /home/airflow/airflow
