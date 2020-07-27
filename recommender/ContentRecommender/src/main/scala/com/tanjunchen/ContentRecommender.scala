package com.tanjunchen

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 * @Author tanjunchen
 * @Date 2020/7/26 16:38
 * @Version 1.0
 */
// 需要的数据源是电影内容信息
case class Movie(mid: Int, name: String, describe: String, timeLong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义电影内容信息提取出的特征向量的电影相似度列表 主要基于 genres
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {
  val TEST_URL = "192.168.17.140"

  // 定义表名和常量
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"
  val SCORE_FILTER = 0.6

  val MONGO_URL = "mongodb://" + TEST_URL + ":27017/recommender"
  val MONGO_DB = "recommender"

  // 设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> MONGO_URL,
      "mongo.db" -> MONGO_DB
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据 并作预处理
    val movieTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // 提取 mid，name，genres 三项作为原始内容特征，分词器默认按照空格做分词
        x => (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c))
      )
      .toDF("mid", "name", "genres")
      .cache()

    /**
     * 核心部分:用 TF-IDF 从内容信息中提取电影特征向量
     */
    // 创建一个分词器，默认按空格分词
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    // 用分词器对原始数据做转换，生成新的一列 words
    val wordsData = tokenizer.transform(movieTagsDF)

    // wordsData.show()

    /**
     * +----+--------------------+--------------------+--------------------+
     * | mid|                name|              genres|               words|
     * +----+--------------------+--------------------+--------------------+
     * |   1|    Toy Story (1995)|Adventure Animati...|[adventure, anima...|
     * |2546|Deep End of the O...|               Drama|             [drama]|
     * |   2|      Jumanji (1995)|Adventure Childre...|[adventure, child...|
     * |   3|Grumpier Old Men ...|      Comedy Romance|   [comedy, romance]|
     * |2548|Rage: Carrie 2, T...|              Horror|            [horror]|
     * |  10|    GoldenEye (1995)|Action Adventure ...|[action, adventur...|
     * |  11|American Presiden...|Comedy Drama Romance|[comedy, drama, r...|
     * |  12|Dracula: Dead and...|       Comedy Horror|    [comedy, horror]|
     * |  13|        Balto (1995)|Adventure Animati...|[adventure, anima...|
     * |2549|Wing Commander (1...|       Action Sci-Fi|    [action, sci-fi]|
     * |  14|        Nixon (1995)|               Drama|             [drama]|
     * |  15|Cutthroat Island ...|Action Adventure ...|[action, adventur...|
     * |2550|Haunting, The (1963)|     Horror Thriller|  [horror, thriller]|
     * |2551| Dead Ringers (1988)|Drama Horror Thri...|[drama, horror, t...|
     * |2552|My Boyfriend's Ba...|              Comedy|            [comedy]|
     * |2553|Village of the Da...|Horror Sci-Fi Thr...|[horror, sci-fi, ...|
     * |2554|Children of the D...|Horror Sci-Fi Thr...|[horror, sci-fi, ...|
     * |2555|Baby Geniuses (1999)|              Comedy|            [comedy]|
     * |  16|       Casino (1995)|         Crime Drama|      [crime, drama]|
     * |2557|I Stand Alone (Se...|      Drama Thriller|   [drama, thriller]|
     * +----+--------------------+--------------------+--------------------+
     *
     */

    // 引入 HashingTF 工具，可以把一个词语序列转化成对应的词频
    // 设置 50 个特征维度
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)

    // 特征化数据
    val featurizedData = hashingTF.transform(wordsData)

    // featurizedData.show(5)
    /** 稀疏向量
     * +----+--------------------+--------------------+--------------------+--------------------+
     * | mid|                name|              genres|               words|         rawFeatures|
     * +----+--------------------+--------------------+--------------------+--------------------+
     * |   1|    Toy Story (1995)|Adventure Animati...|[adventure, anima...|(50,[11,13,19,43,...|
     * |2546|Deep End of the O...|               Drama|             [drama]|     (50,[27],[1.0])|
     * |   2|      Jumanji (1995)|Adventure Childre...|[adventure, child...|(50,[11,13,19],[1...|
     * |   3|Grumpier Old Men ...|      Comedy Romance|   [comedy, romance]|(50,[28,49],[1.0,...|
     * |2548|Rage: Carrie 2, T...|              Horror|            [horror]|     (50,[26],[1.0])|
     * +----+--------------------+--------------------+--------------------+--------------------+
     */

    // 引入 IDF 工具，可以得到 idf 模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    // 训练 idf 模型，得到每个词的逆文档频率
    val idfModel = idf.fit(featurizedData)

    // 用模型对原数据进行处理，得到文档中每个词的 tf-idf 作为新的特征向量
    val rescaledData = idfModel.transform(featurizedData)

    // rescaledData.show(5, truncate = false)

    /**
     * +----+---------------------------------+-------------------------------------------+-------------------------------------------------+-------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
     * |mid |name                             |genres                                     |words                                            |rawFeatures                                |features                                                                                                              |
     * +----+---------------------------------+-------------------------------------------+-------------------------------------------------+-------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
     * |1   |Toy Story (1995)                 |Adventure Animation Children Comedy Fantasy|[adventure, animation, children, comedy, fantasy]|(50,[11,13,19,43,49],[1.0,1.0,1.0,1.0,1.0])|(50,[11,13,19,43,49],[2.5454417340657622,2.6163934700380467,2.0794415416798357,3.0441643356605095,1.0439043437350966])|
     * |2546|Deep End of the Ocean, The (1999)|Drama                                      |[drama]                                          |(50,[27],[1.0])                            |(50,[27],[0.7605551441254693])                                                                                        |
     * |2   |Jumanji (1995)                   |Adventure Children Fantasy                 |[adventure, children, fantasy]                   |(50,[11,13,19],[1.0,1.0,1.0])              |(50,[11,13,19],[2.5454417340657622,2.6163934700380467,2.0794415416798357])                                            |
     * |3   |Grumpier Old Men (1995)          |Comedy Romance                             |[comedy, romance]                                |(50,[28,49],[1.0,1.0])                     |(50,[28,49],[1.7860451679646163,1.0439043437350966])                                                                  |
     * |2548|Rage: Carrie 2, The (1999)       |Horror                                     |[horror]                                         |(50,[26],[1.0])                            |(50,[26],[2.194720551703029])                                                                                         |
     * +----+---------------------------------+-------------------------------------------+-------------------------------------------------+-------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
     */

    val movieFeatures = rescaledData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    )
      .rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )
    // movieFeatures.collect().take(5).foreach(println(_))
    /**
     * (1,[0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 2.5454417340657622; 0.0; 2.6163934700380467; 0.0; 0.0; 0.0; 0.0; 0.0; 2.0794415416798357; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 3.0441643356605095; 0.0; 0.0; 0.0; 0.0; 0.0; 1.0439043437350966])
     * (2546,[0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.7605551441254693; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0])
     * (2,[0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 2.5454417340657622; 0.0; 2.6163934700380467; 0.0; 0.0; 0.0; 0.0; 0.0; 2.0794415416798357; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0])
     * (3,[0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 1.7860451679646163; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 1.0439043437350966])
     * (2548,[0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 2.194720551703029; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0])
     */

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter {
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > SCORE_FILTER) // 过滤出相似度大于0.6的
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 求向量余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
