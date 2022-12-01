# BigData_WS_22-23_OpenCell-ID
## Big-Data Projekt zum Thema OpenCell-ID von Marcel Fleck
-------------------------------------------------------------------------
# How to start the complete Process
## First clone this repo
- Clone Git-Repo: ```git clone https://github.com/24secondss/BigData_WS_22-23_OpenCell-ID.git ```
- Navigate into Git-Folder: ```cd BigData_WS_22-23_OpenCell-ID/ ```

## Start all docker from this repo
- Execute command: ```docker-compose up -d``` (herefore the docker-compose package is needed)

## Need to start hadoop-cluster
- Navigate into Hadoop-Docker-Bash: ```docker exec -it hadoop bash```
- Execute command: ```start-all.sh```

## Airflow DAGs are paused by default (view https://github.com/marcelmittelstaedt/BigData/blob/master/docker/airflow/airflow.cfg)
- Open Browser with [VM's external IP-Adress]:8080
- Unpause DAG "OpenCelliD_full_db" by clicking the icon left of the name
- Wait until this one is done
- Unpause DAG "OpenCelliD_diffs_db"

## Frontend
After the initial DAG is done, you can operate on the Website without errors
  - Open Frontend on [VM's external IP-Adress]:3000
  - Fill inputfields with desired longitute and latitude
  - Wait until all results are displayed
