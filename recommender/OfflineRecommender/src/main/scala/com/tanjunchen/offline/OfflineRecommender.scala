package com.tanjunchen.offline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


// 基于评分数据的 LFM 只需要 rating 数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义基于 LFM 电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

  // 定义表名和常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20
  val MONGODB_URL = "mongodb://192.168.17.140:27017/recommender"
  val MONGO_DB = "recommender"

  // ALS
  val ALS_RANK = 100
  val ALS_ITERATIONS = 10
  val ALS_LAMBDA = 0.1

  val SCORE_FILTER = 0.6

  def main(args: Array[String]): Unit = {


    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> MONGODB_URL,
      "mongo.db" -> MONGO_DB
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 1. 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map {
        // 转换成 rdd, 并且去掉时间戳
        x => (x.uid, x.mid, x.score)
      }.cache()

    // (uid, mid, score)  uid 去重
    // 从 rating 数据中提取所有的 uid 和 mid 并去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // 训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 迭代次数不宜过大
    val model = ALS.train(trainData, ALS_RANK, ALS_ITERATIONS, ALS_LAMBDA)

    // 基于用户和电影的隐特征 计算预测评分 得到用户的推荐列表
    // 计算 user 和 movie 的笛卡尔积，得到一个空评分矩阵
    val userMovies = userRDD.cartesian(movieRDD)
    // 调用 model 的 predict 方法预测评分
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0) // 过滤出评分大于 0 的项
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs
          (uid, recs.toList.sortWith(_._2 > _._2)
            .take(USER_MAX_RECOMMENDATION)
            .map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    // write to mongoDB
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 基于电影隐特征 计算相似度矩阵 得到电影的相似度列表
    val movieFeatures = model.productFeatures.
      map {
        case (mid, features) => (mid, new DoubleMatrix(features))
      }
    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter {
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = this.consineSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > SCORE_FILTER) // 过滤出相似度大于 0.6 SCORE_FILTER 的电影集
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    //
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 求向量余弦相似度
  def consineSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
