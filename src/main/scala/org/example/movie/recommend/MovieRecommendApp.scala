package org.example.movie.recommend

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author apophis
 * @File MovieRecommendApp
 * @Time 2024/4/4 19:53
 * @Description
 */
object MovieRecommendApp {
  def main(args: Array[String]): Unit = {
    // TODO 集群运行注释掉
    System.setProperty("hadoop.home.dir", "D:/dev/winutils/hadoop-3.2.0")
    // 设置运行环境
    val sparkConf: SparkConf = new SparkConf()
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      // TODO 集群运行注释掉
      .master("local[10]")
      .appName("MovieRecommendApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // "us_index_value","it_index_value","fractional_play_count","user_id"
    val ratings: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      // TODO 集群运行 修改为hdfs目录
      .load("output/origin/ratings.csv")


    val userDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      // TODO 集群运行 修改为hdfs目录
      .load("output/origin/user_codes.csv")

    val itemDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      // TODO 集群运行 修改为hdfs目录
      .load("output/origin/item_codes.csv")

    // 使用ALS在训练集上构建推荐模型
    val als: ALS = new ALS()
      // 迭代最大值
      .setMaxIter(5)
      // ALS中正则化参数，默认为1.0
      .setRegParam(0.01)
      .setUserCol("us_index_value")
      .setItemCol("it_index_value")
      .setRatingCol("fractional_play_count")
    // 训练模型
    val model: ALSModel = als.fit(ratings)

    import spark.implicits._
    // 为每个用户生成推荐音乐
    val recommend: DataFrame = model.recommendForAllUsers(3)
      .withColumn("recommendation", explode($"recommendations"))
      .select(
        $"us_index_value",
        $"recommendation.it_index_value",
        $"recommendation.rating"
      )
    val recommend_with_name: DataFrame = recommend.join(userDF, Seq("us_index_value"), "inner")
      .join(itemDF, Seq("it_index_value"), "inner")
    recommend_with_name.select("user_id", "item", "rating").write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/recommendation") // 替换成您的MySQL数据库连接信息
      .option("dbtable", "t_recommend") // 替换成您要写入的表名
      .option("user", "root") // 替换成您的MySQL用户名
      .option("password", "123456") // 替换成您的MySQL密码
      .mode(SaveMode.Append)
      .save()
  }
}
