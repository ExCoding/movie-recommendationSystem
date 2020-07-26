package com.tanjunchen

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @Author tanjunchen
 * @Date 2020/7/26 0:08
 * @Version 1.0
 */

// 定义连接助手对象，序列化
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis(StreamingRecommender.REDIS_URL)
  lazy val mongoClient = MongoClient(MongoClientURI(StreamingRecommender.MONGODB_URL))
}

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义基于 LFM 电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {
  val REDIS_URL = "192.168.17.140"
  val MONGODB_URL = "mongodb://192.168.17.140:27017/recommender"
  val MONGODB_DB = "recommender"

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  val FILTER_SCORE = 0.7
  val SCORE_FACTOR = 3

  val KAFKA_BATCH_TIME = 2
  val KAFKA_URL = "192.168.17.240:9092,192.168.17.241:9092,192.168.17.242:9092"
  val KAFKA_TOPIC = "recommender"
  val KAFKA_GROUP_ID = "recommender"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> MONGODB_URL,
      "mongo.db" -> MONGODB_DB,
      "kafka.topic" -> KAFKA_TOPIC
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 拿到 streaming context
    val sc = spark.sparkContext
    // batch duration
    val ssc = new StreamingContext(sc, Seconds(KAFKA_BATCH_TIME))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载电影相似度矩阵数据，把它广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { movieRecs => // 为了查询相似度方便，转换成map
        (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()

    // 创建一个广播变量
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)


    // 定义 kafka 连接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> KAFKA_URL,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> KAFKA_GROUP_ID,
      "auto.offset.reset" -> "latest"
    )

    // 通过 kafka 创建一个 DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // 把原始数据 UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    // 继续做流式处理，核心实时算法部分
    ratingStream.foreachRDD {
      rdds =>
        rdds.foreach {
          case (uid, mid, score, timestamp) => {
            println("*************** rating data coming! ***************")

            // 1. 从 redis 里获取当前用户最近的 K 次评分，保存成 Array[(mid, score)]
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

            // 2. 从相似度矩阵中取出当前电影最相似的 MAX_SIM_MOVIES_NUM 个电影，作为备选列表，Array[mid]
            val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            // 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
            val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

            // 4. 把推荐数据保存到 mongodb
            saveDataToMongoDB(uid, streamRecs)
          }
        }
    }
    // 开始接收和处理数据
    ssc.start()
    println("*************** streaming started! ***************")

    ssc.awaitTermination()
  }

  // redis 操作返回的是 java 类，为了用 map 操作需要引入转换类

  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从 redis 读取数据，用户评分数据保存在 uid:UID 为 key 的队列里，value 是 MID:SCORE
    jedis.lrange("uid:" + uid, 0, num - 1)
      .map {
        item => // 具体每个评分又是以冒号分隔的两个值
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
   * 获取跟当前电影相似的 num 个电影，作为备选电影
   *
   * @param num       相似电影的数量
   * @param mid       当前电影 ID
   * @param uid       当前评分用户 ID
   * @param simMovies 相似度矩阵
   * @return 过滤之后的备选电影列表
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 1. 从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 2. 从 mongodb 中查询用户已看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item => item.get("mid").toString.toInt
      }

    // 3. 把看过的电影过滤掉，得到输出列表
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
   *
   * @param candidateMovies
   * @param userRecentlyRatings
   * @param simMovies
   * @return
   */
  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // 定义一个 ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义一个 HashMap，保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
      // 拿到备选电影和最近评分电影的相似度
      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)

      if (simScore > FILTER_SCORE) {
        // 计算备选电影的基础推荐得分
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > SCORE_FACTOR) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    // 根据备选电影的 mid 做 groupby，根据公式去求最后的推荐评分
    scores.groupBy(_._1).map {
      // groupBy 之后得到的数据 Map( mid -> ArrayBuffer[(mid, score)] )
      case (mid, scoreList) =>
        // 对应于最后的计算公式
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  // 获取两个电影之间的相似度
  /**
   *
   * @param mid1
   * @param mid2
   * @param simMovies
   * @return
   */
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 求一个数的对数，利用换底公式，底数默认为 10
  /**
   *
   * @param m
   * @return
   */
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  /**
   *
   * @param uid
   * @param streamRecs
   * @param mongoConfig
   */
  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    // 定义到 StreamRecs 表的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    // 如果表中已有 uid 对应的数据，则删除
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // 将 streamRecs 数据存入表中
    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
      "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }
}
