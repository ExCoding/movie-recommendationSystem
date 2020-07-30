# 电影推荐系统

## 演示效果

**[演示视频](https://www.bilibili.com/video/BV1Xi4y137BU)**

## 环境搭建

### ES

```
docker run  -p 9200:9200 -p 9300:9300 --name='es' -d  -v /home/k8s-develop/movie/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml elasticsearch:5.6.8

docker exec -it es /bin/bash    # 进入交互模式，es必须先启动

## 如果报错, 可能涉及以下知识点

sudo vim /etc/sysctl.conf

vm.max_map_count=655360

docker rm $(docker ps -q -f status=exited)
```

elasticsearch.yml 内容为：

```yaml
http.host: 0.0.0.0
# Uncomment the following lines for a production cluster deployment
transport.host: 0.0.0.0
#discovery.zen.minimum_master_nodes: 1
```

进入 Docker 容器中运行 DataLoader 后校验运行结果：

curl http://192.168.17.140:9200/

```json
{
  "name" : "pYkk1Sm",
  "cluster_name" : "elasticsearch",
  "cluster_uuid" : "Aoy9VQPpSk62trl1m0T4KA",
  "version" : {
    "number" : "5.6.8",
    "build_hash" : "688ecce",
    "build_date" : "2018-02-16T16:46:30.010Z",
    "build_snapshot" : false,
    "lucene_version" : "6.6.1"
  },
  "tagline" : "You Know, for Search"
}
```

curl http://192.168.17.140:9200/recommender/_search?pretty

```json
{
  "took" : 5,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 2791,
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "2542",
        "_score" : 1.0,
        "_source" : {
          "mid" : 2542,
          "name" : "Lock, Stock & Two Smoking Barrels (1998)",
          "describe" : "A card sharp and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.",
          "timeLong" : "105 minutes",
          "issue" : "August 31, 1999",
          "shoot" : "1998",
          "language" : "English",
          "genres" : "Comedy|Crime|Thriller",
          "actors" : "Jason Flemyng|Dexter Fletcher|Nick Moran|Jason Statham|Vinnie Jones|Sting|Steven Mackintosh|Nicholas Rowe|Lenny McLean|P.H. Moriarty|Frank Harper|Huggy Leaver|Stephen Marcus|Peter McNicholl|Nick Marcq|Tony McMahon|Steve Sweeney|Charles Forbes|Vas Blackwood|Jake Abraham|Victor McGuire|Danny John-Jules|Elwin 'Chopper' David|Vera Day|Rob Brydon|Alan Ford|Andrew Tiernan|Jason Flemyng|Dexter Fletcher|Nick Moran|Jason Statham|Vinnie Jones",
          "directors" : "Guy Ritchie",
          "tags" : "organized crime|dark comedy|Guy Ritchie"
        }
      }
    ]
  }
}
```

### Mongo

```
docker run --name mongo -p 27017:27017 -d mongo

docker ps -a
```

```shell script
k8s-develop@ubuntu:~/movie$ docker ps -a
CONTAINER ID        IMAGE                 COMMAND                  CREATED             STATUS              PORTS                                            NAMES
4163257ed28b        elasticsearch:5.6.8   "/docker-entrypoint.…"   14 minutes ago      Up 14 minutes       0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp   es
f2e35a3bcd8d        mongo                 "docker-entrypoint.s…"   19 minutes ago      Up 19 minutes       0.0.0.0:27017->27017/tcp                         mongo
```

### Redis

```
docker run  -d --name redis -p 6379:6379 redis:4.0.2

docker ps -a

docker exec -it redis  bash

root@efc549d33e89:/data# redis-
redis-benchmark  redis-check-aof  redis-check-rdb  redis-cli        redis-sentinel   redis-server     

redis-cli

lpush uid:2 261:4.0 265:5.0 266:5.0 272:3.0 273:4.0 292:3.0 296:4.0 300:3.0

127.0.0.1:6379> keys *
(empty list or set)
127.0.0.1:6379> lpush uid:2 261:4.0 265:5.0 266:5.0 272:3.0 273:4.0 292:3.0 296:4.0 300:3.0
(integer) 8
127.0.0.1:6379> keys *
1) "uid:2"
127.0.0.1:6379> lrange uid:2 0 -1
1) "300:3.0"
2) "296:4.0"
3) "292:3.0"
4) "273:4.0"
5) "272:3.0"
6) "266:5.0"
7) "265:5.0"
8) "261:4.0"
127.0.0.1:6379>
```

### Zookeeper

下载 zookeeper-3.4.5.tar.gz，并且解压。

cp conf/zoo_sample.cfg zoo.cfg

启动 zookeeper ./bin/zkServer.sh start

```
bin/zkServer.sh start
cp conf/zoo_sample.cfg  conf/zoo.cfg  
root@ubuntu:/usr/local/src/zookeeper-3.4.5# bin/zkServer.sh status
JMX enabled by default
Using config: /usr/local/src/zookeeper-3.4.5/bin/../conf/zoo.cfg
Mode: standalone
```

### Kafka 

下载 kafka_2.11-0.10.2.1.tgz，并且解压。

启动 ./bin/kafka-server-start.sh -daemon ./config/server.properties。

```
查看 topic 

./bin/kafka-topics.sh --list -zookeeper localhost:2181

创建 topic

./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic log

./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic recommender

查看 topic

./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic recommender
```

### Flume

下载 apache-flume-1.6.0-bin.tar.gz，解压 apache-flume-1.6.0-bin.tar.gz。

在 apache-flume-1.6.0-bin/conf 文件下，添加 log-kafka.properties 文件。

启动 flume 监听 kafka ./bin/flume-ng agent -c ./conf/ -f ./conf/log-kafka.properties -n a1 -Dflume.root.logger=INFO,console

```
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -f /usr/local/src/apache-tomcat-8.5.57/logs/catalina.out
a1.sources.exectail.interceptors=i1
a1.sources.exectail.interceptors.i1.type=regex_filter

a1.sources.exectail.interceptors.i1.regex=.+MOVIE_RATING_PREFIX.+
a1.sources.exectail.channels = memoryChannel

a1.sinks.k1.type = logger
# 设置kafka接收器
#a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
# 设置kafka的broker地址和端口号
#a1.sinks.k1.brokerList=192.168.17.140:9092
# 设置Kafka的topic
#a1.sinks.k1.topic=log
# 设置序列化的方式
#a1.sinks.k1.serializer.class=kafka.serializer.StringEncoder

# use a channel which buffers events in memory
a1.channels.c1.type=memory
a1.channels.c1.capacity = 100000
a1.channels.c1.transactionCapacity = 1000

# Bind the source and sink to the channel
a1.sources.r1.channels=c1
a1.sinks.k1.channel=c1
```

运行数据离线模块服务后，查看结果。

docker exec -it mongo /bin/bash

```
> show databases;
admin        0.000GB
config       0.000GB
local        0.000GB
recommender  0.005GB
> use recommender
switched to db recommender
> show tables;
AverageMovies
GenresTopMovies
Movie
RateMoreMovies
RateMoreRecentlyMovies
Rating
Tag
> db.AverageMovies.findOne()
{
	"_id" : ObjectId("5f19b6e37f2f925a7c455f91"),
	"mid" : 2542,
	"avg" : 4.128378378378378
}
> db.GenresTopMovies.findOne()
{
	"_id" : ObjectId("5f19b6ea7f2f925a7c456a12"),
	"genres" : "Western",
	"recs" : [
		{
			"mid" : 1254,
			"score" : 4.3
		},
		{
			"mid" : 1209,
			"score" : 4.21875
		},
		{
			"mid" : 1304,
			"score" : 4.173333333333333
		},
		{
			"mid" : 3037,
			"score" : 4.166666666666667
		},
		{
			"mid" : 1283,
			"score" : 4.119047619047619
		},
		{
			"mid" : 2070,
			"score" : 4.038461538461538
		},
		{
			"mid" : 26258,
			"score" : 4
		},
		{
			"mid" : 106762,
			"score" : 4
		},
		{
			"mid" : 1181,
			"score" : 4
		},
		{
			"mid" : 2055,
			"score" : 4
		}
	]
}
> 
```

## 测试部署流程

测试实时推荐的流程：

    1. 启动上述的 ES、Mongo、Redis、Zookeeper、Kafka、Flume。
    
    2. 运行各个模块。注意替换代码中使用的 TEST_URL 地址信息。
      ContentRecommender
      DataLoader
      KafkaStream
      OfflineRecommender
      StatisticsRecommender
      StreamingRecommender
    3. 将 businessServer 编译打成 war 包，并且放置 tomcat 中运行。
      具体详情请参考视频。
    4. 注册用户，并且评分进行测试，查看后台相关日志，如 Kafka、tomcat、StreamingRecommender、KafkaStream 等日志。

案例测试结果：

kafka 生产者, uid = 2 对某部电影进行模拟评价

```
[root@master kafka_2.11-0.10.2.1]# ./bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic recommender
2|161|3.0|835355493
```

redis
```
127.0.0.1:6379> lrange uid:2 0 -1
1) "300:3.0"
2) "296:4.0"
3) "292:3.0"
4) "273:4.0"
5) "272:3.0"
6) "266:5.0"
7) "265:5.0"
8) "261:4.0"
127.0.0.1:6379> 
```

mongo
```
{ "_id" : ObjectId("5f1d34487f2f927018417f0b"), "uid" : 2, "recs" : [ { "mid" : 100553, "score" : 3.372539756021398 }, { "mid" : 1277, "score" : 3.358318909483622 }, { "mid" : 1880, "score" : 3.3360708872282214 }, { "mid" : 112175, "score" : 3.3214684311068963 }, { "mid" : 1440, "score" : 3.3143326283220538 }, { "mid" : 27808, "score" : 3.295601747795067 }, { "mid" : 26819, "score" : 3.2754158085818217 }, { "mid" : 1672, "score" : 3.2519903807386084 }, { "mid" : 2045, "score" : 3.241762257171785 }, { "mid" : 107081, "score" : 3.2185618305292394 }, { "mid" : 106471, "score" : 3.2185618274095393 }, { "mid" : 106762, "score" : 3.218561825720072 }, { "mid" : 106473, "score" : 3.218561822106436 }, { "mid" : 1427, "score" : 3.217805603507783 }, { "mid" : 2561, "score" : 3.2075089914593997 }, { "mid" : 104913, "score" : 3.1991672476124497 }, { "mid" : 1631, "score" : 3.1842857773078825 }, { "mid" : 1881, "score" : 3.0985064818083012 }, { "mid" : 127124, "score" : 3.076520040478509 }, { "mid" : 2852, "score" : 3.057268591688964 } ] }
``` 

项目日志
```
                                                                                2020-07-26 15:43:40,041   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding enable.auto.commit to false for executor
2020-07-26 15:43:40,047   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding auto.offset.reset to none for executor
2020-07-26 15:43:40,049   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding executor group.id to spark-executor-recommender
2020-07-26 15:43:40,050   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding receive.buffer.bytes to 65536 see KAFKA-3135
*************** streaming started! ***************
*************** rating data coming! ***************
```

kafka 生产者, uid = 2 对某部电影进行模拟评价

打分数据

```
[root@master kafka_2.11-0.10.2.1]# ./bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic recommender
2|161|3.0|835355493
2|165|5.0|835355441
```

redis

```
127.0.0.1:6379> lrange uid:2 0 -1
1) "165:5.0"
2) "300:3.0"
3) "296:4.0"
4) "292:3.0"
5) "273:4.0"
6) "272:3.0"
7) "266:5.0"
8) "265:5.0"
9) "261:4.0"
127.0.0.1:6379>
```

mongo

```
> db.StreamRecs.find()
{ "_id" : ObjectId("5f1d35467f2f927018417f0c"), "uid" : 2, "recs" : [ { "mid" : 100553, "score" : 3.372539756021398 }, { "mid" : 1665, "score" : 3.3539450757188805 }, { "mid" : 112175, "score" : 3.3214684311068963 }, { "mid" : 27808, "score" : 3.295601747795067 }, { "mid" : 26819, "score" : 3.2754158085818217 }, { "mid" : 2788, "score" : 3.2678983268260158 }, { "mid" : 1324, "score" : 3.160093371467256 }, { "mid" : 27728, "score" : 3.0327338020264243 }, { "mid" : 127052, "score" : 2.943341024508923 }, { "mid" : 121126, "score" : 2.943341024508923 }, { "mid" : 102666, "score" : 2.943341024508923 }, { "mid" : 107412, "score" : 2.943341024508923 }, { "mid" : 118468, "score" : 2.943341024508923 }, { "mid" : 1036, "score" : 2.894235855218782 }, { "mid" : 2423, "score" : 2.8433388274896942 }, { "mid" : 285, "score" : 2.7977429468827886 }, { "mid" : 1270, "score" : 2.706466611161512 }, { "mid" : 30848, "score" : 2.66842812959567 }, { "mid" : 27851, "score" : 2.6684281283547637 }, { "mid" : 1370, "score" : 2.3326278647529515 } ] }
```

项目日志

```
                                                                              2020-07-26 15:43:40,041   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding enable.auto.commit to false for executor
2020-07-26 15:43:40,047   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding auto.offset.reset to none for executor
2020-07-26 15:43:40,049   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding executor group.id to spark-executor-recommender
2020-07-26 15:43:40,050   WARN --- [                                              main]  org.apache.spark.streaming.kafka010.KafkaUtils                                  (line:   66)  :  overriding receive.buffer.bytes to 65536 see KAFKA-3135
*************** streaming started! ***************
*************** rating data coming! ***************
*************** rating data coming! ***************
```

可以明显地看到推荐数据发生明显的变化：

    { "_id" : ObjectId("5f1d35467f2f927018417f0c"), "uid" : 2, "recs" : [ { "mid" : 100553, "score" : 3.372539756021398 }, { "mid" : 1665, "score" : 3.3539450757188805 }, { "mid" : 112175, "score" : 3.3214684311068963 }, { "mid" : 27808, "score" : 3.295601747795067 }, { "mid" : 26819, "score" : 3.2754158085818217 }, { "mid" : 2788, "score" : 3.2678983268260158 }, { "mid" : 1324, "score" : 3.160093371467256 }, { "mid" : 27728, "score" : 3.0327338020264243 }, { "mid" : 127052, "score" : 2.943341024508923 }, { "mid" : 121126, "score" : 2.943341024508923 }, { "mid" : 102666, "score" : 2.943341024508923 }, { "mid" : 107412, "score" : 2.943341024508923 }, { "mid" : 118468, "score" : 2.943341024508923 }, { "mid" : 1036, "score" : 2.894235855218782 }, { "mid" : 2423, "score" : 2.8433388274896942 }, { "mid" : 285, "score" : 2.7977429468827886 }, { "mid" : 1270, "score" : 2.706466611161512 }, { "mid" : 30848, "score" : 2.66842812959567 }, { "mid" : 27851, "score" : 2.6684281283547637 }, { "mid" : 1370, "score" : 2.3326278647529515 } ] }
    { "_id" : ObjectId("5f1d34487f2f927018417f0b"), "uid" : 2, "recs" : [ { "mid" : 100553, "score" : 3.372539756021398 }, { "mid" : 1277, "score" : 3.358318909483622 }, { "mid" : 1880, "score" : 3.3360708872282214 }, { "mid" : 112175, "score" : 3.3214684311068963 }, { "mid" : 1440, "score" : 3.3143326283220538 }, { "mid" : 27808, "score" : 3.295601747795067 }, { "mid" : 26819, "score" : 3.2754158085818217 }, { "mid" : 1672, "score" : 3.2519903807386084 }, { "mid" : 2045, "score" : 3.241762257171785 }, { "mid" : 107081, "score" : 3.2185618305292394 }, { "mid" : 106471, "score" : 3.2185618274095393 }, { "mid" : 106762, "score" : 3.218561825720072 }, { "mid" : 106473, "score" : 3.218561822106436 }, { "mid" : 1427, "score" : 3.217805603507783 }, { "mid" : 2561, "score" : 3.2075089914593997 }, { "mid" : 104913, "score" : 3.1991672476124497 }, { "mid" : 1631, "score" : 3.1842857773078825 }, { "mid" : 1881, "score" : 3.0985064818083012 }, { "mid" : 127124, "score" : 3.076520040478509 }, { "mid" : 2852, "score" : 3.057268591688964 } ] }

## 项目结构

### 主要模块

#### 数据源解析模块

![](docs/images/主要表信息.png)

movies 电影信息

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| mid | Int | 电影的 ID |
| name | String | 电影的名称 |
| descri | String | 电影的描述 |
| timelong | String | 电影的时长 |
| shoot | String | 电影拍摄时间 |
| issue | String | 电影发布时间 |
| language | String | 电影语言 |
| genres | String | 电影所属类别 |
| director | String | 电影的导演 |
| actors | String | 电影的演员 |

ratings 用户评分信息

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| uid | Int |  用户的 ID |   |
| mid | Int |  电影的 ID |   |
| score | Double | 电影的分值 |   |
| timestamp | Long | 评分的时间 |   |

tags 电影标签信息

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| uid | Int |  用户的 ID |   |
| mid | Int |  电影的 ID |   |
| tag | String | 电影的标签 |   |
| timestamp | Long | 评分的时间 |   |

user 信息

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| uid |  Int |  用户的 ID |
|  username |  String  | 用户名 |  
|  password |  String |  用户密码 |
|  first |  boolean |  用于是否第一次登录 |
|  genres |  List<String> |  用户偏爱的电影类型 |
|  timestamp |  Long |  用户创建的时间 |

RateMoreMoviesRecently 最近电影评分个数统计表

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| mid | Int | 电影的 ID | 
| count | Int | 电影的评分数 | 
| yearmonth | String | 评分的时段|  yyyymm

RateMoreMovies 电影评分个数统计表

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
| mid | Int | 电影的 ID | 
| count | Int | 电影的评分数| 

AverageMoviesScore 电影平均评分表

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
|  mid |  Int |  电影的 ID |  
|  avg |  Double |  电影的平均评分 |  

MovieRecs 电影相似性矩阵

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
|  mid |  Int |  电影的 ID |  |
|  recs |  Array[(mid:Int,score:Double)] |  该电影最相似的电影集合 |  |  

UserRecs 用户电影推荐矩阵

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
|  uid |  Int |  用户的 ID |  |  
|  recs |  Array[(mid:Int,score:Double)] |  推荐给该用户的电影集合 |  |  

StreamRecs 用户实时电影推荐矩阵

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
|  uid |  Int |  用户的 ID |  |  
|  recs |  Array[(mid:Int,score:Double)] |  实时推荐给该用户的电影集合 |  |  

GenresTopMovies 电影类别 TOP 10

| 字段名 | 字段类型 | 字段描述 | 字段备注 |
|  ----  | ---- |  ----  | ---- |
|  genres |  String |  电影类型 |  |  
|  recs |  Array[(mid:Int,score:Double)] |  TOP10 电影 |  |  

#### 统计推荐模块

![](docs/images/统计推荐模块.png)

#### 离线推荐模块

离线推荐服务主要分为统计性算法、基于 ALS 的协同过滤推荐算法以及基于内容推荐算法。

##### 用户电影推荐矩阵 (User-Item)

通过 ALS 训练出来的 Model 来计算所有当前用户电影的推荐矩阵

```
1. UserId 和 MovieID 做笛卡尔积，产生<uid，mid>的元组
2. 通过模型预测<uid，mid>的元组。
3. 将预测结果通过预测分值进行排序。
4. 返回分值最大的 K 个电影，作为当前用户的推荐。
```

##### 电影相似度矩阵 (Item-Item)

通过 ALS 计算电影间相似度矩阵，该矩阵用于查询当前电影的相似电影并为实时推荐系统提供基础服务。

```
离线计算的 ALS 算法，算法最终会为用户、电影分别生成最终的特征矩阵，分别是表示用户特征矩阵的 U(m x k)矩阵，每个用户由 k 个特征描述;
表示物品特征矩阵的 V(n x k)矩阵，每个物品也由 k 个特征描述。所以，每个电影用 V(n x k)每一行的 向量表示其特征,
于是任意两个电影 p：特征向量为,电影 q：特征向量为之间的相似度 sim(p,q)可以使用 和 的余弦值来表示;
```

余弦相似度公式

![余弦相似度公式](docs/images/余弦相似度公式.png)

常见的相似度<系数>算法

1. 余弦相似度<Cosine Similarity>
1. 皮尔森相关系数<Pearson Correlation Coefficient>
1. Jaccard相似系数<Jaccard Coefficient>
1. 对数似然相似率
1. 互信息/信息增益，相对熵/KL散度
1. Tanimoto系数<广义Jaccard相似系数>
1. 信息检索 - 词频-逆文档频率<TF-IDF>
1. 词对相似度 - 点间相似度

使用 OfflineRecommender 模块 ALSTrainer 计算最佳 ALS 的超参数。

```
def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 这里指定迭代次数为 20，rank 和 lambda 在几个值中选取调整
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        // 计算当前参数对应模型的 rmse，返回 Double
        val model = ALS.train(trainData, rank, 20, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // 按照 rmse 排序
    // 控制台打印输出最优参数
    println("===>", result.sortBy(_._3).head)
    // println(result.minBy(_._3))
    // (===>,(100,0.1,0.9267907826304898))
  }
```

如果迭代次数过大，本地机器配置不高，设置的迭代次数不宜过大。

OfflineRecommender 离线模块，保存用户电影推荐信息、电影相似度信息到 MongoDB。

![](docs/images/离线推荐模块.png)

ALS (最小二乘法)

![](docs/images/ALS.png)

Item-Item 

![](docs/images/ii.png)

User-Item

![](docs/images/ui.png)

#### 实时推荐模块

![](docs/images/实时推荐模块.png)

![](docs/images/实时推荐1.png)

![](docs/images/实时推荐2.png)

#### 基于内容的推荐模块

![](docs/images/基于内容的推荐.png)

基于内容推荐的测试结果：

运行 ContentRecommender 模块

mongo 查看数据

db.ContentMovieRecs.findOne()

```
{
	"_id" : ObjectId("5f1d48be7f2f92736c8d4bc6"),
	"mid" : 1084,
	"recs" : [
		{
			"mid" : 16,
			"score" : 1
		},
		{
			"mid" : 30,
			"score" : 1
		},
		{
			"mid" : 117,
			"score" : 1
		},
		{
			"mid" : 245,
			"score" : 1
		},
		{
			"mid" : 247,
			"score" : 1
		}
        ......
	]
}
```

#### 混合算法推荐

![](docs/images/混合推荐.png)

#### 其他算法

LR、中文分词、决策树

## 系统设计

![](docs/images/数据生命周期.png)

### 项目架构

![](docs/images/项目架构.png)

### 系统模块设计

![](docs/images/系统模块设计.png)

### 技术栈

ES + Kafka + Flume + Spark + SparkStreaming + ML + Mongo + Redis + Spring

## 参考

参考视频 [电影推荐系统](https://www.bilibili.com/video/BV1vJ411X7Sz?from=search&seid=3840241043752401873)

## 总结

待续

## 下一步

如何优化？？？

1. 部署
1. 推荐算法
1. 代码
1. ......