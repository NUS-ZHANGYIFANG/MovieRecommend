# project-spark-movie-recommend-system


## create mysql table

```shell
# make sure mysql is started
systemctl restart mysqld.service
```

Enter the mysql command line

```shell
mysql -uroot -p123456
```

Execute init.sql

```text
use recommendation;
SOURCE /output/init.sql;
```


## Clean data

Execute the data sorting/data_process.py program  
**Execute commands in linux**

```shell
python3 /output/data_process.py
```

## Cluster exclusive start cluster

```shell
# Start hadoop cluster
sh /export/software/hadoop-3.2.0/sbin/start-hadoop.sh
# Start spark cluster
sh /export/software/spark-3.1.2-bin-hadoop3.2/sbin/start-all.sh
```

## Cluster-specific upload files to hdfs

```shell
# Create hdfs directory
hdfs dfs -mkdir -p /data/output/origin/
# Clear hdfs directory
hdfs dfs -rm -r /data/output/origin/*
# Upload files to hdfs directory
hdfs dfs -put /output/origin/item_codes.csv /data/output/origin/
hdfs dfs -put /output/origin/ratings.csv /data/output/origin/
hdfs dfs -put /output/origin/user_codes.csv /data/output/origin/

```

## Calculate the most popular movies

**Excuting**

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--executor-memory 512m \
--num-executors 4 \
--executor-cores 1 \
--class org.example.movie.recommend.PopMovieTopApp \
/output/origin/movie-recommend-jar-with-dependencies.jar
```

## Calculate offline recommendation results

**Excuting**

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--executor-memory 512m \
--num-executors 4 \
--executor-cores 1 \
--class org.example.movie.recommend.MovieRecommendApp \
/output/origin/movie-recommend-jar-with-dependencies.jar
```

## Start kafka and create topic

```shell
# Start kafka
sh /export/software/kafka_2.12-2.8.2/bin/zookeeper-server-start.sh  -daemon /export/software/kafka_2.12-2.8.2/config/zookeeper.properties
sh /export/software/kafka_2.12-2.8.2/bin/kafka-server-start.sh -daemon /export/software/kafka_2.12-2.8.2/config/server.properties
# Create topic
sh /export/software/kafka_2.12-2.8.2/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test_topic
# producer
sh /export/software/kafka_2.12-2.8.2/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test_topic
# input to producer 
# wenzel
# aaaaa
# Go to mysql to find recommended results
select * from recommendation.t_recommend_realtime;

```

## Real-time recommendations

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--executor-memory 512m \
--num-executors 4 \
--executor-cores 1 \
--class org.example.movie.recommend.MovieRecommendRealTimeApp \
/output/origin/movie-recommend-jar-with-dependencies.jar
```

task stop command

```shell
yarn application -kill application_1713623434486_0006
```

## Cluster exclusive Close the cluster

```shell
# Shut down the spark cluster
sh /export/software/spark-3.1.2-bin-hadoop3.2/sbin/stop-all.sh
# Shut down the hadoop cluster
sh /export/software/hadoop-3.2.0/sbin/stop-hadoop.sh
```

## Shut down kafka

```shell
# Shut down kafka
sh /export/software/kafka_2.12-2.8.2/bin/kafka-server-stop.sh
sh /export/software/kafka_2.12-2.8.2/bin/zookeeper-server-stop.sh
```