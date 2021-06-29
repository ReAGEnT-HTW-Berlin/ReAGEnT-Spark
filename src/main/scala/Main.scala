import Analysis._
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples.tweets_bundestag_legislatur?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()


    val tweets = MongoSpark.load(sparkSession)
    tweets.createOrReplaceTempView("tweets")

    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd

    countTotal(rdd, saveToDB = true)
    countByHashtag(rdd, saveToDB = true)
    avgTweetLength(rdd, saveToDB = true)
    avgReplies(rdd, saveToDB = true)
    mostTweetsDay(rdd, saveToDB = true)
    mostTweetsTime(rdd, saveToDB = true)
    avgLikes(rdd, saveToDB = true)
    mediaUsage(rdd, saveToDB = true)
    mostTaggedUsers(rdd, saveToDB = true)
    mostActiveUsers(rdd, saveToDB = true)
    totalReplies(rdd, saveToDB = true)
    avgRetweets(rdd, saveToDB = true)
    countUrls(rdd, saveToDB = true)


    println("Hello World")
  }

}
