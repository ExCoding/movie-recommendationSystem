package com.tanjunchen.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticsRecommender {

  case class Movie(mid: Int, name: String, describe: String, timelong: String, issue: String,
                   shoot: String, language: String, genres: String, actors: String, directors: String)

  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

  case class MongoConfig(uri: String, db: String)

  // 定义一个基准推荐对象
  case class Recommendation(mid: Int, score: Double)

  // 定义电影类别 top10 推荐对象
  case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // 统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"
  val MONGO_DB = "recommender"

  val TOTAL_NUM = 10

  val MONGO_URL = "mongodb://192.168.17.140:27017/recommender"


  def main(args: Array[String]): Unit = {


    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> MONGO_URL,
      "mongo.db" -> MONGO_DB
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 从 mongodb 加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // 创建名为 ratings 的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO
    // 1. 历史热门统计 历史评分数据最多 mid,count
    // 2. 近期热门统计 按照 "yyyyMM" 格式选取最近的评分数据 统计评分个数  创建一个日期格式化工具 udf
    // 3. 优质电影统计 统计电影的平均评分 mid avg
    // 4. 各类别电影 Top 统计

    // 1. 历史热门统计 历史评分数据最多 mid,count
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count_num from ratings group by mid")

    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2. 近期热门统计 按照 "yyyyMM" 格式选取最近的评分数, 统计评分个数,创建一个日期格式化工具 udf

    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf，把时间戳转换成年月格式
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 对原始数据做预处理 去掉 uid
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // 从 ratingOfMonth 中查找电影在各个月份的评分，mid，count，yearmonth
    val rateMoreRecentlyMoviesDF = spark
      .sql("select mid, count(mid) as count, yearmonth from ratingOfMonth " +
        "group by yearmonth, mid order by yearmonth desc, count desc")

    // 存入 mongodb
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. 优质电影统计 统计电影的平均评分 mid avg
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. 各类别电影 Top 统计 定义所有类别
    // 这里理论上应该从数据库或者存储设备中获取数据
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    // 把平均评分加入 movie 表里，加一列，inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // 为做笛卡尔积，把 genres 转成 rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // 计算类别 top10，首先对类别和电影做 笛卡尔积
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        case (genre, moiveRow) => moiveRow.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase())
      }.map {
      case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
    }.groupByKey()
      .map {
        // 将整个数据集的数据量减小，生成 RDD[String,Iter[mid,avg]]
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(TOTAL_NUM)
          .map(item => Recommendation(item._1, item._2)))
      }.toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

  /**
   * 将 DataFrame 存入 MongoDB
   *
   * @param df
   * @param collection_name
   * @param mongoConfig
   */
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
