# Runbook

## MVP Flow

1. Collector receives raw WebSocket messages.
2. Collector publishes raw messages to Kafka.
3. Consumer stores raw messages in S3 Bronze.
4. Clean Lambda writes Silver trade and daily Parquet outputs.
5. Signal Lambda writes Gold signal history and publishes alerts.

## Reprocessing

Reprocessing should start from the Bronze raw logs because they preserve the original WebSocket messages.


1+2 터미널 동시 진행
```bash
# 1
PS C:\Users\kkmj2\OneDrive\문서\DE-Proj> python -m src.collector.main
```
```bash
PS C:\Users\kkmj2\OneDrive\문서\DE-Proj> python -m src.consumer.to_bronze
```
```bash
PS C:\Users\kkmj2\OneDrive\문서\DE-Proj> docker ps
CONTAINER ID   IMAGE                    COMMAND                  CREATED          STATUS                            PORTS                                         NAMES
f638e712c71e   apache/airflow:2.10.5    "/usr/bin/dumb-init …"   35 seconds ago   Up 4 seconds (health: starting)   8080/tcp                                      airflow-scheduler
5ff311158135   apache/airflow:2.10.5    "/usr/bin/dumb-init …"   35 seconds ago   Up 4 seconds (health: starting)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp   airflow-webserver
783dea2bc53f   provectuslabs/kafka-ui   "/bin/sh -c 'java --…"   35 seconds ago   Up 33 seconds                     0.0.0.0:8081->8080/tcp, [::]:8081->8080/tcp   de-proj-kafka-ui-1
daa484a35d14   wurstmeister/kafka       "start-kafka.sh"         35 seconds ago   Up 34 seconds                     0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp   kafka
59e2b52e9cb2   postgres:13              "docker-entrypoint.s…"   35 seconds ago   Up 34 seconds (healthy)           5432/tcp                                      airflow-postgres
3f00825a86f7   wurstmeister/zookeeper   "/bin/sh -c '/usr/sb…"   35 seconds ago   Up 34 seconds                     0.0.0.0:2181->2181/tcp, [::]:2181->2181/tcp   zookeeper
PS C:\Users\kkmj2\OneDrive\문서\DE-Proj> docker-compose down
[+] down 8/8
 ✔ Container airflow-scheduler  Removed                                                                             2.4s
 ✔ Container de-proj-kafka-ui-1 Removed                                                                             2.0s
 ✔ Container airflow-webserver  Removed                                                                             3.0s
 ✔ Container kafka              Removed                                                                             1.7s
 ✔ Container airflow-init       Removed                                                                             0.3s
 ✔ Container airflow-postgres   Removed                                                                             0.5s
 ✔ Container zookeeper          Removed                                                                             1.4s
 ✔ Network de-proj_default      Removed                                                                             0.3s
PS C:\Users\kkmj2\OneDrive\문서\DE-Proj> docker-compose up -d
[+] up 8/8
 ✔ Network de-proj_default      Created                                                                             0.0s
 ✔ Container airflow-postgres   Healthy                                                                             7.9s
 ✔ Container zookeeper          Started                                                                             1.2s
 ✔ Container airflow-init       Exited                                                                             25.0s
 ✔ Container kafka              Started                                                                             1.5s
 ✔ Container de-proj-kafka-ui-1 Started                                                                             1.8s
 ✔ Container airflow-scheduler  Started                                                                            25.1s
 ✔ Container airflow-webserver  Started                                                                            25.2s
 ```