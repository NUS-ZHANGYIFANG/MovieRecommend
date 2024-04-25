package org.example.movie.recommend

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @File MovieRecommendApp
 * @Time 2024/4/4 19:53
 * @Description
 */
object MovieRecommendApp {
  def main(args: Array[String]): Unit = {
    // Test
    //System.setProperty("hadoop.home.dir", "D:/dev/winutils/hadoop-3.2.0")
    // Set up the operating environment
    val sparkConf: SparkConf = new SparkConf()
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      // Test
      //.master("local[10]")
      .appName("MovieRecommendApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // "us_index_value","it_index_value","fractional_play_count","user_id"
    val ratings: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("/data/output/origin/ratings.csv")


    val userDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("/data/output/origin/user_codes.csv")

    val itemDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("/data/output/origin/item_codes.csv")

    // Use ALS to build a recommendation model on the training set
    val als: ALS = new ALS()
      // Iteration maximum
      .setMaxIter(5)
      // Regularization parameter in ALS, default is 1.0
      .setRegParam(0.01)
      .setUserCol("us_index_value")
      .setItemCol("it_index_value")
      .setRatingCol("fractional_play_count")
    // Training model
    val model: ALSModel = als.fit(ratings)

    import spark.implicits._
    // Generate 3 recommended movies for each user
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
      .option("url", "jdbc:mysql://172.30.32.3:3306/recommendation")
      .option("dbtable", "t_recommend")
      .option("user", "root")
      .option("password", "123456")
      .mode(SaveMode.Append)
      .save()
  }
}
