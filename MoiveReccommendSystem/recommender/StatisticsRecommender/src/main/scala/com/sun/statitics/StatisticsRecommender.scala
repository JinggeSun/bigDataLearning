package com.sun.statitics

import java.text.SimpleDateFormat
import java.util.Date

import com.sun.statitics.config.MongoConfig
import com.sun.statitics.entity.{GenresRecommendation, Movie, Recommendation}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object StatisticsRecommender {

  //从mongodb中获取的表
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //sparkConfig
    val  sparkConfig = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    // sparksession
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    //mongo配置
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加入隐式转换
    import spark.implicits._

    // 从mongodb中加载数据
    val movieDF = spark
        .read
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_MOVIE_COLLECTION)
        .format("com.mongodb.spark.sql")
        .load()
        .as[Movie]
        .toDF()

    val ratingDF = spark
        .read
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_RATING_COLLECTION)
        .format("com.mongodb.spark.sql")
        .load()
        .as[Movie]
        .toDF()

    //创建1张视图 ratings
    ratingDF.createOrReplaceTempView("ratings")

    /**
     * 统计电影历史数据中 每个电影的评分数量
     * A:被评论100次
     * B:被评论200次
     */
    // 数据结构 mid，count
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count from ratings group by mid")

    //保存到数据库中
    rateMoreMoviesDF
        .write
        .option("uri",mongoConfig.uri)
        .option("collection",RATE_MORE_MOVIES)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    /**
     * 统计以月为单位，统计每个电影的评分数量
     */

    //日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp装成年月
    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)
    // 将与啊来的rating数据集 中的时间转换为年月的形式
    val ratingOfYearMonth = spark.sql("select mid,score,changeDate(timestamp) as yeahmouth from rating")
    // 将新的数据集转换为视图
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies = spark.sql("select mid,count(mid) as count,yeahmouth from ratingOfMouth group by yeahmouth,mid")
    // 保存
    rateMoreRecentlyMovies
        .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    /**
     * 统计每个电影的平均得分
     */

    val averageMoviesDF = spark.sql("select mod,avg(score) as avg from ratings group by mid")
    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    /**
     * 统计每种电影类型中评分最高的10个电影  评分是指平均分
     */
    val movieWithScore = movieDF.join(averageMoviesDF,Seq("mid","mid"))

    //电影类别，理应从数据库中获取
    val genres = List("Action","Adventure")

    // RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    //top10
    val genernTopMovies = genresRDD.cartesian(movieWithScore.rdd)
        .filter{
          case (genres,row)=> row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase())
        }
        .map{
          case (genres,row) => (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
        }
        .groupByKey()
        .map{
          case (genres,items) => GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1,item._2)))
        }
        .toDF()


    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 结束
    spark.stop()

  }
}