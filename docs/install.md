# 数据加载模块

## ES

```
docker run  -p 9200:9200 -p 9300:9300 --name='es' -d  -v /home/k8s-develop/movie/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml elasticsearch:5.6.8

docker exec -it es /bin/bash    #进入交互模式，es必须先启动

sudo vim /etc/sysctl.conf

vm.max_map_count=655360

docker rm $(docker ps -q -f status=exited)
```

elasticsearch.yml

```yaml
http.host: 0.0.0.0
# Uncomment the following lines for a production cluster deployment
transport.host: 0.0.0.0
#discovery.zen.minimum_master_nodes: 1
```

运行 DataLoader 后校验运行结果

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
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "3062",
        "_score" : 1.0,
        "_source" : {
          "mid" : 3062,
          "name" : "Longest Day, The (1962)",
          "describe" : "The retelling of June 6, 1944, from the perspectives of the Germans, US, British, Canadians, and the Free French. Marshall Erwin Rommel, touring the defenses being established as part of the Reich's Atlantic Wall, notes to his officers that when the Allied invasion comes they must be stopped on the beach. 'For the Allies as well as the Germans, it will be the longest day'",
          "timeLong" : "178 minutes",
          "issue" : "November 2, 1999",
          "shoot" : "1962",
          "language" : "English|Français|Deutsch",
          "genres" : "Action|Drama|War",
          "actors" : "Eddie Albert|Paul Anka|Arletty|Jean-Louis Barrault|Richard Beymer|Hans Christian Blech|Bourvil|Richard Burton|Wolfgang Büttner|Red Buttons|Sean Connery|Ray Danton|Pauline Carton|Armin Dahlen|Mark Damon|Richard Dawson|Irina Demick|Fred Dur|Fabian|Mel Ferrer|Frank Finlay|Henry Fonda|Steve Forrest|Bernard Fox|Robert Freitag|Bernard Fresson|Gert Fröbe|Lutz Gabor|Arnold Gelderman|Leo Genn|Harold Goodwin|Walter Gotell|Henry Grace|John Gregson|Clément Harari|Paul Hartmann|Ruth Hausmeister|Jack Hedley|Michael Hinz|Werner Hinz|Donald Houston|Jeffrey Hunter|Karl John|Curd Jürgens|Til Kiwe|Alexander Knox|Peter Lawford|Wolfgang Lukschy|Christian Marquand|Roddy McDowall|Sal Mineo|Robert Mitchum|Kenneth More|Richard Münch|Edmond O'Brien|Leslie Phillips|Wolfgang Preiss|Heinz Reincke|Madeleine Renaud|Robert Ryan|Dietmar Schönherr|Ernst Schröder|George Segal|Jean Servais|Hans Söhnker|Rod Steiger|Richard Todd|Tom Tryon|Peter van Eyck|Vicco von Bülow |Robert Wagner|John Wayne|Stuart Whitman|Eddie Albert|Paul Anka|Arletty|Jean-Louis Barrault|Richard Beymer",
          "directors" : "Bernhard Wicki|Ken Annakin|Andrew Marton"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "2971",
        "_score" : 1.0,
        "_source" : {
          "mid" : 2971,
          "name" : "All That Jazz (1979)",
          "describe" : "Bob Fosse's semi-autobiographical film celebrates show business stripped of glitz or giddy illusions. Joe Gideon (Roy Scheider) is at the top of the heap, one of the most successful directors and choreographers in musical theatre. But he can feel his world slowly collapsing around him--his obsession with work has almost destroyed his personal life, and only his bottles of pills keep him going.",
          "timeLong" : "123 minutes",
          "issue" : "August 19, 2003",
          "shoot" : "1979",
          "language" : "English",
          "genres" : "Drama|Fantasy|Musical",
          "actors" : "Roy Scheider|Jessica Lange|Leland Palmer|Ann Reinking|Ben Vereen|Cliff Gorman|Erzsebet Foldi|Michael Tolan|Max Wright|William LeMassena|Irene Kane|Deborah Geffner|Kathryn Doby|Anthony Holland|Robert Hitt|David Margulies|Sue Paul|Keith Gordon|Frankie Man|Alan Heim|John Lithgow|Sandahl Bergman|CCH Pounder|Wallace Shawn|Roy Scheider|Jessica Lange|Leland Palmer|Ann Reinking|Ben Vereen",
          "directors" : "Bob Fosse"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "3087",
        "_score" : 1.0,
        "_source" : {
          "mid" : 3087,
          "name" : "Scrooged (1988)",
          "describe" : "In this modern take on Charles Dickens' 'A Christmas Carol,' Frank Cross (Bill Murray) is a wildly successful television executive whose cold ambition and curmudgeonly nature has driven away the love of his life, Claire Phillips (Karen Allen). But after firing a staff member, Eliot Loudermilk (Bobcat Goldthwait), on Christmas Eve, Frank is visited by a series of ghosts who give him a chance to re-evaluate his actions and right the wrongs of his past.",
          "timeLong" : "101 minutes",
          "issue" : "November 9, 1999",
          "shoot" : "1988",
          "language" : "English",
          "genres" : "Comedy|Fantasy|Romance",
          "actors" : "Bill Murray|Karen Allen|John Forsythe|Bobcat Goldthwait|Carol Kane|Robert Mitchum|Michael J. Pollard|Alfre Woodard|John Glover|David Johansen|Nicholas Phillips|Mabel King|John Murray|Wendie Malick|Brian Doyle-Murray|Joel Murray|Delores Hall|Jamie Farr|Buddy Hackett|Robert Goulet|John Houseman|Lee Majors|Mary Lou Retton|Maria Riva|Anne Ramsey|Sydna Scott|Miles Davis|Larry Carlton|David Sanborn|Paul Schaffer|Mary Ellen Trainor|Al 'Red Dog' Weber|Robert Hammond|Susan Isaacs|Bill Murray|Karen Allen|John Forsythe|Bobcat Goldthwait|Carol Kane",
          "directors" : "Richard Donner"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "2200",
        "_score" : 1.0,
        "_source" : {
          "mid" : 2200,
          "name" : "Under Capricorn (1949)",
          "describe" : "In 1831, Irishman Charles Adare travels to Australia to start a new life with the help of his cousin who has just been appointed governor. When he arrives he meets powerful landowner and ex-convict Sam Flusky, who wants to do a business deal with him. Whilst attending a dinner party at Flusky's house, Charles meets Flusky's wife Henrietta who he had known as a child back in Ireland. Henrietta is an alcoholic and seems to be on the verge of madness.",
          "timeLong" : "108 minutes",
          "issue" : "June 17, 2003",
          "shoot" : "1949",
          "language" : "English",
          "genres" : "Drama",
          "actors" : "Ingrid Bergman|Joseph Cotten|Michael Wilding|Margaret Leighton|Cecil Parker|Denis O'Dea|Jack Watling|Harcourt Williams|John Ruddock|Bill Shine|Victor Lucas|Alfred Hitchcock|Ingrid Bergman|Joseph Cotten|Michael Wilding|Margaret Leighton|Cecil Parker",
          "directors" : "Alfred Hitchcock"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "103221",
        "_score" : 1.0,
        "_source" : {
          "mid" : 103221,
          "name" : "Not Suitable for Children (2012)",
          "describe" : "A young playboy who learns he has one month until he becomes infertile sets out to procreate as much as possible.",
          "timeLong" : "97 minutes",
          "issue" : "",
          "shoot" : "2012",
          "language" : "English",
          "genres" : "Comedy|Romance",
          "actors" : "Ryan Kwanten|Bojana Novaković|Laura Brent|Alice Parkinson|Sarah Snook|Ryan Corr|Ryan Kwanten|Bojana Novaković|Laura Brent|Alice Parkinson|Sarah Snook",
          "directors" : "Peter Templeman"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "1591",
        "_score" : 1.0,
        "_source" : {
          "mid" : 1591,
          "name" : "Spawn (1997)",
          "describe" : "After being murdered by corrupt colleagues in a covert government agency, Al Simmons (Michael Jai White) makes a pact with the devil to be resurrected to see his beloved wife Wanda (Theresa Randle). In exchange for his return to Earth, Simmons agrees to lead Hell's Army in the destruction of mankind.",
          "timeLong" : "96 minutes",
          "issue" : "January 6, 1998",
          "shoot" : "1997",
          "language" : "English",
          "genres" : "Action|Adventure|Sci-Fi|Thriller",
          "actors" : "Michael Jai White|Martin Sheen|John Leguizamo|Theresa Randle|Nicol Williamson|D. B. Sweeney|Melinda Clarke|Miko Hughes|Sydni Beaudoin|Frank Welker|Michael Jai White|Martin Sheen|John Leguizamo|Theresa Randle|Nicol Williamson",
          "directors" : "Mark A.Z. Dippé"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "1645",
        "_score" : 1.0,
        "_source" : {
          "mid" : 1645,
          "name" : "Devil's Advocate, The (1997)",
          "describe" : "A hotshot lawyer gets more than he bargained for when he learns his new boss is Lucifer himself.",
          "timeLong" : "144 minutes",
          "issue" : "December 1, 1998",
          "shoot" : "1997",
          "language" : "English|Français|Deutsch|Italiano|普通话|Español",
          "genres" : "Drama|Mystery|Thriller",
          "actors" : "Keanu Reeves|Al Pacino|Charlize Theron|Jeffrey Jones|Judith Ivey|Connie Nielsen|Craig T. Nelson|Tamara Tunie|Ruben Santiago-Hudson|Debra Monk|Vyto Ruginis|Laura Harrington|Pamela Gray|George Wyner|Chris Bauer|Connie Embesi|Jonathan Cavallary|Heather Matarazzo|Murphy Guyer|Leo Burmester|Bill Moor|Neal Jones|Eddie Aldridge|Mark Deakins|Rony Clanton|George O. Gore II|Alan Manson|Brian Poteat|Daniel Oreskes|Kim Chan|Caprice Benedetti|Don King|Ray Garvey|Rocco Musacchia|Susan Kellermann|James Saito|Harsh Nayyar|Mohammed Ghaffari|Nicki Cochrane|Fenja Klaus|Gino Lucci|Novella Nelson|Vincent Laresca|Benny Nieves|Franci Leary|Monica Keena|Gloria Lynne Henry|Keanu Reeves|Al Pacino|Charlize Theron|Jeffrey Jones|Judith Ivey",
          "directors" : "Taylor Hackford"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "1959",
        "_score" : 1.0,
        "_source" : {
          "mid" : 1959,
          "name" : "Out of Africa (1985)",
          "describe" : "Out of Africa tells the story of the life of Danish author Karen Blixen, who at the beginning of the 20th century moved to Africa to build a new life for herself. The film is based on the autobiographical novel by Karen Blixen from 1937.",
          "timeLong" : "161 minutes",
          "issue" : "February 29, 2000",
          "shoot" : "1985",
          "language" : "English|Kiswahili",
          "genres" : "Drama|Romance",
          "actors" : "Meryl Streep|Robert Redford|Klaus Maria Brandauer|Michael Kitchen|Malick Bowens|Joseph Thiaka|Stephen Kinyanjui|Michael Gough|Suzanna Hamilton|Rachel Kempson|Graham Crowden|Leslie Phillips|Mike Bugara|Shane Rimmer|Job Seda|Mohammed Umar|Iman|Meryl Streep|Robert Redford|Klaus Maria Brandauer|Michael Kitchen|Malick Bowens",
          "directors" : "Sydney Pollack"
        }
      },
      {
        "_index" : "recommender",
        "_type" : "Movie",
        "_id" : "2366",
        "_score" : 1.0,
        "_source" : {
          "mid" : 2366,
          "name" : "King Kong (1933)",
          "describe" : "An adventure film about a film crew in search of a monster on a remote island. The crew finds King Kong and decides to take him back to New York as a money making spectacle. The film is a masterpiece of Stop-Motion in filmmaking history and inspired a line of King Kong films.",
          "timeLong" : "100 minutes",
          "issue" : "November 22, 2005",
          "shoot" : "1933",
          "language" : "English",
          "genres" : "Action|Adventure|Fantasy|Horror",
          "actors" : "Robert Armstrong|Fay Wray|Bruce Cabot|Frank Reicher|Victor Wong|James Flavin|Sam Hardy|Noble Johnson|Steve Clemente|Bill Williams|Dick Curtis|Roscoe Ates|Merian C. Cooper|Ernest B. Schoedsack|Robert Armstrong|Fay Wray|Bruce Cabot|Frank Reicher|Victor Wong",
          "directors" : "Merian C. Cooper|Ernest B. Schoedsack"
        }
      }
    ]
  }
}
```

## Mongo

```
docker run --name mongo -p 27017:27017 -d mongo

docker ps -a
```

## 结果

```shell script
k8s-develop@ubuntu:~/movie$ docker ps -a
CONTAINER ID        IMAGE                 COMMAND                  CREATED             STATUS              PORTS                                            NAMES
4163257ed28b        elasticsearch:5.6.8   "/docker-entrypoint.…"   14 minutes ago      Up 14 minutes       0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp   es
f2e35a3bcd8d        mongo                 "docker-entrypoint.s…"   19 minutes ago      Up 19 minutes       0.0.0.0:27017->27017/tcp                         mongo
```

# 数据离线模块

运行数据离线模块服务后，查看结果

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

# 离线推荐模块

## 用户电影推荐矩阵 (User-Item)

通过 ALS 训练出来的 Model 来计算所有当前用户电影的推荐矩阵

```
1. UserId 和 MovieID 做笛卡尔积，产生<uid，mid>的元组
2. 通过模型预测<uid，mid>的元组。
3. 将预测结果通过预测分值进行排序。
4. 返回分值最大的 K 个电影，作为当前用户的推荐。
```

## 电影相似度矩阵

通过 ALS 计算电影间相似度矩阵，该矩阵用于查询当前电影的相似电影并为实时推荐系统提供基础服务。

```
离线计算的 ALS 算法，算法最终会为用户、电影分别生成最终的特征矩阵，分别是表示用户特征矩阵的 U(m x k)矩阵，每个用户由 k 个特征描述;
表示物品特征矩阵的 V(n x k)矩阵，每个物品也由 k 个特征描述。所以，每个电影用 V(n x k)每一行的 向量表示其特征,
于是任意两个电影 p：特征向量为,电影 q：特征向量为之间的相似度 sim(p,q)可以使用 和 的余弦值来表示;
```

余弦相似度公式

![余弦相似度](images/余弦相似度公式.png)

常见的相似度<系数>算法

1. 余弦相似度<Cosine Similarity>
1. 皮尔森相关系数<Pearson Correlation Coefficient>
1. Jaccard相似系数<Jaccard Coefficient>
1. 对数似然相似率
1. 互信息/信息增益，相对熵/KL散度
1. Tanimoto系数<广义Jaccard相似系数>
1. 信息检索 - 词频-逆文档频率<TF-IDF>
1. 词对相似度 - 点间相似度