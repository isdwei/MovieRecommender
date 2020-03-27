package com.dwei.recommender


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoDB}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


/**
  * 建立样例类
  * Movie: 电影ID，电影名称，详情描述，时长，发行时间，拍摄语言，类型，演员表，导演
  *          int                         String
  * Rating:用户ID，电影ID，评分，评分时间
  * Tag: 用户ID，电影ID， 标签， 标签时间
  */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

//数据库配置
case class MongoConfig(uri:String, db:String)
case class MySQLConfig(uri:String,db:String)

/**
  *
  * @param httpHosts            http主机列表
  * @param transportHosts       transport主机列表
  * @param index                需要操作的索引
  * @param clustername          集群名称
  */
case class ESConfig(httpHosts:String, transportHosts:String, index:String,
                    clustername:String)

object DataLoader {


  val MOVIE_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val MOVIE_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH="E:\\JavaWork\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop001:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop001:9200",
      "es.transportHosts" -> "hadoop001:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"

    )

    //初始化SparkConf,SparkSession
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    sparkConf.set("es.nodes.wan.only","true")

    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    //加载数据
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)

    val movieDF = movieRDD.map(
      item => {
        val fields = item.split("\\^")
        //样例类自动转换为DF
        Movie(fields(0).toInt, fields(1).trim, fields(2).trim, fields(3).trim, fields(4).trim,
          fields(5).trim, fields(6).trim, fields(7).trim, fields(8).trim, fields(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    implicit val mogoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    implicit val mysqlConfig=MySQLConfig("hadoop001","recommender")

    //数据保存
    storeDataInMongoDB(movieDF,ratingDF,tagDF)



    //数据预处理
    import org.apache.spark.sql.functions._
    /**
      * Tag: 用户ID，电影ID， 标签， 标签时间
      * mid -> tag1,tag2,...
      */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    /**
      * moive left join newTags
      */
    val movieWithTagDF = movieDF.join(newTag,Seq("mid"),"left")

    implicit val esConfig = ESConfig(config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get)

    storeDataInES(movieWithTagDF)

//    spark.stop()
  }



  def storeDataInMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    //新建一个连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //写入DF数据
    writeToMongo(movieDF,MONGODB_MOVIE_COLLECTION)
    writeToMongo(ratingDF,MONGODB_RATING_COLLECTION)
    writeToMongo(tagDF,MONGODB_TAG_COLLECTION)
//    movieDF.write
//      .option("uri",mongoConfig.uri)
//      .option("collection",MONGODB_MOVIE_COLLECTION)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()
//    ratingDF.write
//      .option("uri",mongoConfig.uri)
//      .option("collection",MONGODB_RATING_COLLECTION)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()
//    tagDF.write
//      .option("uri",mongoConfig.uri)
//      .option("collection",MONGODB_TAG_COLLECTION)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

  }

  def writeToMongo(df:DataFrame,collection:String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  def storeDataInES(movieDF:DataFrame)(implicit  esConfig: ESConfig): Unit = {
    val settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    //先清理遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index))
      .actionGet()
      .isExists
    ){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    movieDF
      .write
      .option("es.nodes",esConfig.httpHosts)
      .option("es.http.timeout","1000m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index+"/"+ES_MOVIE_INDEX)



  }
}
