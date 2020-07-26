package com.tanjunchen

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

/**
 * @Author tanjunchen
 * @Date 2020/7/26 0:08
 * @Version 1.0
 */

// 定义连接助手对象，序列化
object ConnHelper extends Serializable {
  val REDIS_URL = "192.168.17.140"
  val MONGODB_URL = "mongodb://192.168.17.140:27017/recommender"
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI(MONGODB_URL))
}

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义基于 LFM 电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"




}
