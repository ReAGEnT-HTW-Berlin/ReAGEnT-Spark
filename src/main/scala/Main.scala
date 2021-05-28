import Analysis._
import Utilities._
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      //.config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.SampleData?authSource=examples")
//      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.bson-gaertner?authSource=examples")
      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets_bundestag_aktuelle_legislaturperiode?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()


    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")


    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd

    val t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1)/1000000000.0 + "s")


//    countTotalByHourAndPartyAndBoth(rdd)
//
//    println("Anzahl Tweets gesamt: " + rdd.count())
//
//    println("Erster Tweet: " + rdd.first())
//
//    countTotalByHourAndParty(rdd)
//    countTotalByParty(rdd)
//    countTotalByHour(rdd)
//
//    countByHashtagAndParty(rdd)
//    countByHashtag(rdd)
//    countHashtagsUsedByParty(rdd)
//
//    countByNounsAndParty(rdd)
//
//    countBySource(rdd)
//
//    countConnectedHashtags(rddWithoutRetweets)
//
//    avgTweetLengthByParty(rddWithoutRetweets)
//    avgTweetLengthByTime(rddWithoutRetweets)
//    avgTweetLengthByTimeAndParty(rddWithoutRetweets)


    val t3 = System.nanoTime()
    println("Elapsed time: " + (t3 - t2)/1000000000.0 + "s")
    println("Hello World")
  }

}
