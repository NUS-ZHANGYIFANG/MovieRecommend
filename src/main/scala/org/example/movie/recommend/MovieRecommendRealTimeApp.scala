package org.example.movie.recommend

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{broadcast, col, when}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

import java.util.Properties

/**
 * @File MovieRecommendRealTimeApp
 * @Time 2024/4/4 19:53
 * @Description 基于QueryRunner和HikariConfig实现的实时推荐
 */
object MovieRecommendRealTimeApp {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:/dev/winutils/hadoop-3.2.0")
    // Set up the operating environment
    val sparkConf: SparkConf = new SparkConf()
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      //.master("local[10]")
      .appName("MovieRecommendRealTimeApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val kafkaSourceDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.30.32.3:9092")
      .option("subscribe", "test_topic")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("cast(value AS STRING) as user_id")

    //Define a custom ForeachWriter
    // HikariCP connection pool configuration
    val jdbcUrl = "jdbc:mysql://172.30.32.3:3306/recommendation"
    val username = "root"
    val password = "123456"
    val userRecommendSql: String =
      """
        |insert into t_recommend_realtime
        |SELECT ? as user_id,item,rating from t_recommend where user_id=? order by rating desc limit 3;
        |""".stripMargin

    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password", password)
    // Determine whether the user is a new user
    val userIndexDF: DataFrame = spark.read.jdbc(jdbcUrl, "user_codes", properties)

    val userDF: DataFrame = kafkaSourceDf.join(broadcast(userIndexDF), Seq("user_id"), "left_outer")
      .withColumn("user_id_fix", when(col("us_index_value").isNull, "000000").otherwise(col("user_id")))


    userDF.writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[Row] {
        private var runner: QueryRunner = _

        override def open(partitionId: Long, epochId: Long): Boolean = {
          val hikariConfig = new HikariConfig()
          hikariConfig.setJdbcUrl(jdbcUrl)
          hikariConfig.setUsername(username)
          hikariConfig.setPassword(password)
          hikariConfig.addDataSourceProperty("cachePrepStmts", "true")
          hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
          hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
          runner = new QueryRunner(new HikariDataSource(hikariConfig))
          true
        }
        //Consume data from Kafka through Spark Streaming, then read the recommendation results from
        // the MySQL database based on the received user ID, and write the results to
        // the real-time recommendation table.
        override def process(value: Row): Unit = {
          val user_id: String = value.getAs[String]("user_id")
          val user_id_fix: String = value.getAs[String]("user_id_fix")
          println(s"接收到 user_id: ${user_id} user_id_fix: ${user_id_fix}")
          runner.update(userRecommendSql, user_id, user_id_fix)
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start()
      .awaitTermination()


  }
}
