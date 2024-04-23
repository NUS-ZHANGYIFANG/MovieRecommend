# project-spark-movie-recommend-system


## 创建 mysql 表

```shell
# 请确保mysql已启动
systemctl restart mysqld.service
```

进入mysql命令行

```shell
mysql -uroot -p123456
```

执行 init.sql

```text
use recommendation;
SOURCE /output/init.sql;
```


## 清洗数据

执行 数据整理/data_process.py 程序  
**在linux中的执行命令**

```shell
python3 /output/data_process.py
```

## 集群专属 启动集群

```shell
# 启动hadoop集群
sh /export/software/hadoop-3.2.0/sbin/start-hadoop.sh
# 启动spark集群
sh /export/software/spark-3.1.2-bin-hadoop3.2/sbin/start-all.sh
```

## 集群专属 上传文件到 hdfs

```shell
# 创建hdfs目录
hdfs dfs -mkdir -p /data/output/origin/
# 清空hdfs目录
hdfs dfs -rm -r /data/output/origin/*
# 上传文件到hdfs目录
hdfs dfs -put /output/origin/item_codes.csv /data/output/origin/
hdfs dfs -put /output/origin/ratings.csv /data/output/origin/
hdfs dfs -put /output/origin/user_codes.csv /data/output/origin/

```

## 计算最流行电影

**执行命令**

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

## 计算离线推荐结果

**执行命令**

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

## 启动kafka,创建topic

```shell
# 开启kafka
sh /export/software/kafka_2.12-2.8.2/bin/zookeeper-server-start.sh  -daemon /export/software/kafka_2.12-2.8.2/config/zookeeper.properties
sh /export/software/kafka_2.12-2.8.2/bin/kafka-server-start.sh -daemon /export/software/kafka_2.12-2.8.2/config/server.properties
# 创建topic
sh /export/software/kafka_2.12-2.8.2/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test_topic
# 生产者
sh /export/software/kafka_2.12-2.8.2/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test_topic
# 向生产者中输入 
# wenzel
# aaaaa
# 前往mysql中查找推荐结果 select * from recommendation.t_recommend_realtime;

```

## 实时推荐

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

任务停止命令

```shell
yarn application -kill application_1713623434486_0006
```

## 集群专属 关闭集群

```shell
# 关闭spark集群
sh /export/software/spark-3.1.2-bin-hadoop3.2/sbin/stop-all.sh
# 关闭hadoop集群
sh /export/software/hadoop-3.2.0/sbin/stop-hadoop.sh
```

## 关闭kafka

```shell
# 关闭kafka
sh /export/software/kafka_2.12-2.8.2/bin/kafka-server-stop.sh
sh /export/software/kafka_2.12-2.8.2/bin/zookeeper-server-stop.sh
```