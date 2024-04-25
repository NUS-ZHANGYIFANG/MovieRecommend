package org.example.movie.recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @File MovieRecommendApp
 * @Time 2024/4/4 19:53
 * @Description
 */
object PopMovieTopApp {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:/dev/winutils/hadoop-3.2.0")
    // Set up the operating environment
    val sparkConf: SparkConf = new SparkConf()
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
     // .master("local[10]")
      .appName("PopMovieTopApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // us_index_value,it_index_value,fractional_play_count,user_id
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("/data/output/origin/ratings.csv")
      .createOrReplaceTempView("rating")

    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("/data/output/origin/item_codes.csv")
      .createOrReplaceTempView("item")

    val data: DataFrame = spark.sql(
      """
        |SELECT
        |a.user_id,
        |b.item,
        |a.rating
        |FROM (
        |SELECT
        |	'000000' as user_id,
        |    it_index_value,
        |    AVG(fractional_play_count) AS rating
        |FROM (
        |    SELECT
        |        it_index_value,
        |        fractional_play_count,
        |        ROW_NUMBER() OVER (PARTITION BY it_index_value ORDER BY fractional_play_count) AS row_num_asc,
        |        ROW_NUMBER() OVER (PARTITION BY it_index_value ORDER BY fractional_play_count DESC) AS row_num_desc
        |    FROM rating
        |) a
        |WHERE row_num_asc > 1 AND row_num_desc > 1
        |GROUP BY it_index_value
        |order by AVG(fractional_play_count) desc
        |limit 10
        |) a
        |join item b
        |on a.it_index_value=b.it_index_value
        |""".stripMargin)

    data.write
      .format("jdbc")
      .option("url", "jdbc:mysql://172.30.32.3:3306/recommendation")
      .option("dbtable", "t_recommend")
      .option("user", "root")
      .option("password", "123456")
      .mode(SaveMode.Append)
      .save()

  }
}
