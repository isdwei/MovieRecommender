package com.dwei.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis



object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("hadoop001")
  lazy val mongoClient = MongoClient( MongoClientURI("mongodb://hadoop001:27017/recommender") )
}
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int )
case class MongoConfig(uri:String, db:String)

case class Recommendation( mid: Int, score: Double )

// 定义基于预测评分的用户推荐列表
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object StreamingRecommender {

  //读取近期数据个数
  val MAX_USER_RATINGS_NUM = 20
  //备选电影表电影个数
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {

    println("----------------------------------start---------------------------------------")
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop001:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    println("---------------------------------1---------------------------------------")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingRecommender")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    println("----------------------------------2---------------------------------------")

    // 拿到streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))    // batch duration
//    println("----------------------------------3---------------------------------------")

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    println("----------------------------------4---------------------------------------")


    //出现 Query failed with error code -5 and error message 'Cursor 149606731376 not found on server hadoop001:27017' on server hadoop001:27017
        //游标超时
        val simMovieMatrix = spark.read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[MovieRecs]
          .rdd
          .map(movieRecs =>
            (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap))
          .collectAsMap()



    val simMovieMatrixCast = sc.broadcast(simMovieMatrix)
    println("----------------------------------5---------------------------------------")
    //kafka定义参数
    val kafkaParam=Map(
      "bootstrap.servers" -> "hadoop001:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    println("----------------------------------start---------------------------------------")

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )
    //评分流
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    //核心算法
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) => {
          println("rating data coming! >>>>>>>>>>>>>>>>")
          //redis中获取最近评分--->Array(mid,score)
          val userRecentlyRatings=getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

          //与当前电影最相似的电影列表
          val candidateMovies=getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMovieMatrixCast.value)

          //计算优先级，实时推荐
          val streamRecs=computeMovieScores(candidateMovies,userRecentlyRatings,simMovieMatrixCast.value)

          //保存
          saveDataToMongoDB(uid,streamRecs)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
  import scala.collection.JavaConversions._
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int,Double)] ={
    //从redis读取
    jedis.lrange("uid:"+uid,0,num-1)
      .map{
        item=>
          val attr = item.split("\\:")
          (attr(0).trim.toInt,attr(1).toDouble)
      }
      .toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] ={
    //拿到所有电影
    val allSimMovies=simMovies(mid).toArray
    //从Mongo中查已看过的电影
    val ratingExist=ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid"->uid))
      .toArray
      .map{
        item=>item.get("mid").toString.toInt
      }

    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2>_._2)
      .take(num)
      .map(x=>x._1)
  }

  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={

    val scores = scala.collection.mutable.ArrayBuffer[(Int,Double)]()

    val increMap=scala.collection.mutable.HashMap[Int,Int]()
    val decreMap=scala.collection.mutable.HashMap[Int,Int]()

    for(candidateMovie<-candidateMovies;userRecentlyRating<-userRecentlyRatings){
      val simScore= getMoviesSimScore(candidateMovie,userRecentlyRating._1,simMovies)
      if(simScore > 0.7){
        scores += ((candidateMovie,simScore*userRecentlyRating._2))
        if(userRecentlyRating._2>3){
          increMap(candidateMovie)=increMap.getOrDefault(candidateMovie,0)+1;
        }else{
          decreMap(candidateMovie)=decreMap.getOrDefault(candidateMovie,0)+1;
        }
      }
    }

    scores.groupBy(_._1).map{
      case(mid,scoreList)=>
        ( mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)) )
    }.toArray
  }


  // 获取两个电影之间的相似度
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,scala.collection.immutable.Map[Int, Double]]): Double ={
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 求一个数的对数，利用换底公式，底数默认为10
  def log(m: Int): Double ={
    val N = 10
    math.log(m)/ math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // 定义到StreamRecs表的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // 如果表中已有uid对应的数据，则删除
    streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
    // 将streamRecs数据存入表中
    streamRecsCollection.insert( MongoDBObject(
      "uid"->uid,
      "recs"-> streamRecs.map(x=>MongoDBObject( "mid"->x._1, "score"->x._2 )) )
    )
  }


  }
