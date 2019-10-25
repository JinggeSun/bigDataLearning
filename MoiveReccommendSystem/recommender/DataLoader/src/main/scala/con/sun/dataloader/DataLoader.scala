package con.sun.dataloader

import java.net.InetAddress

import com.mongodb.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import con.sun.dataloader.config.{ESConfig, MongoConfig}
import con.sun.dataloader.entity.{Movie, Rating, Tag}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object DataLoader {

  //定义离线文件路径
  val MOVIE_DATA_PATH = ""
  val RATING_DATA_PATH = ""
  val TAG_DATA_PATH = ""

  //定义要存储的表
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    // 新建一个到mongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果mongoDB中有对应的数据库，那么就应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 保存
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwirte")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

      tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwirte")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据库表创建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"-> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid"-> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid"-> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"-> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"-> 1))

    //关闭连接
    mongoClient.close()
  }

  def storeDataInES(movieWithTagsDF: DataFrame)(implicit esConfig : ESConfig) = {
    // 新建一个配置
    val settings:Settings = Settings.builder()
      .put("cluster.name",esConfig.clustername).build()

    //新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)
    // 将transportHosts 添加到esclient中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    // 清楚之前的数据
    //需要清除掉ES中遗留的数据
    if(esClient.admin().indices().exists(new
        IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入到ES中
    movieWithTagsDF
      .write
      .option("es.nodes",esConfig.httpHost)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index+"/"+ES_MOVIE_INDEX)

    esClient.close()
  }


  def main(args: Array[String]): Unit = {

    //定义一个map，填写配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.url" -> "mongodb://127.0.0.1/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "127.0.0.1:9200",
      "es.transportHosts" -> "127.0.0.1:9300",
      "es.index" -> "recommmender",
      "es.cluster.name" -> "es-cluster"
    )

    //1. spakrconf
    val sparkConfig = new SparkConf().setMaster(config("spark.cores"))
    // sparksession
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    //引入包
    import spark.implicits._
    //2. 加载数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // 将RDD转为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

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


    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 将数据保存到mongodb中
    storeDataInMongoDB(movieDF,ratingDF,tagDF)

    import org.apache.spark.sql.functions._

    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|",collect_set($"tag")))
      .as("tags")
      .select("mid","tags")

    // 将处理后的tag数据，和moive数据融合，产生新的movie的数据
    val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")

    // es配置的隐式参数
    implicit val esConfig = ESConfig(config("es.httpHosts"),config("es.transportHosts"),config("es.index"),config("es.cluster.name"))

    //将新movie保存到es中
    storeDataInES(movieWithTagsDF)

    //关闭spark
    spark.stop()
  }
















}
