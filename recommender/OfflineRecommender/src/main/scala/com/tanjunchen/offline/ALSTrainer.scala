package com.tanjunchen.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import com.tanjunchen.offline.OfflineRecommender.{MONGODB_RATING_COLLECTION, MONGODB_URL, MONGO_DB}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 对于我们的模型, 这并不一定是最优的参数选取, 所以我们需要对模型进行评估。
 * 通常的做法是计算均方根误差（RMSE）,考察预测评分与实际评分之间的误差。
 * 有了 RMSE，我们可以就可以通过多次调整参数值，来选取 RMSE 最小的一组作为我们模型的优化选择。
 */
object ALSTrainer {

  val SPLIT_TRAIN_PARAMETER = 0.8
  val SPLIT_TEST_PARAMETER = 0.2

  def main(args: Array[String]): Unit = {

    // 设置日志级别
    //    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

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

    // 加载评分数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // 转化成rdd，并且去掉时间戳

    ratingRDD.cache()

    // 随机切分数据集，生成训练集和测试集
    val splits = ratingRDD.randomSplit(Array(SPLIT_TRAIN_PARAMETER, SPLIT_TEST_PARAMETER))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // 模型参数选择，输出最优参数
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  /**
   * adjustALSParams 方法是模型评估的核心，输入一组训练数据和测试数据，输出计算得到最小 RMSE 的那组参数。
   *
   * @param trainData
   * @param testData
   */
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 这里指定迭代次数为 5，rank 和 lambda 在几个值中选取调整
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(0.1, 0.01, 0.001))
      yield {
        // 计算当前参数对应模型的 rmse，返回 Double
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // 按照 rmse 排序
    // 控制台打印输出最优参数
    println("===>", result.sortBy(_._3).head)
    // println(result.minBy(_._3))
  }

  /**
   * getRMSE 计算 REMS 均方根误差
   *
   * @param model
   * @param data
   * @return
   */
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 计算预测评分
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // 以 uid，mid 作为外键，inner join 实际观测值和预测值
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    // 内连接得到(uid, mid),(actual, predict)
    sqrt(
      observed.join(predict).map {
        case ((_, _), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
