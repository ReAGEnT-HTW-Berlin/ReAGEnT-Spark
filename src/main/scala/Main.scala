import Analysis._
import Utilities._
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import org.apache.spark.sql.{Row, SparkSession}

import java.time.LocalDateTime

object Main {

  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      //      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.bson-gaertner?authSource=examples")
      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets_bundestag_aktuelle_legislaturperiode?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()


    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + "s")


    val tweets = MongoSpark.load(sparkSession)
    tweets.createOrReplaceTempView("tweets")

    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd


    val t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1) / 1000000000.0 + "s")


    val referenceTime = LocalDateTime.now().minusDays(7).toString.splitAt(10)._1

    val tweetsSinceX = sparkSession.sql("SELECT * FROM tweets WHERE date >= \"" + referenceTime + "\"")



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


    //rdd.map(x => getTime(x)).filter(_.toString())

    //tweetsSinceX aber als Rdd[Document]
    val tweetsSinceXV2 = rdd.filter(_.get("created_at").toString.splitAt(10)._1 > referenceTime).cache()

    val t3 = System.nanoTime()
    println("Elapsed time Filter: " + (t3 - t2) / 1000000000.0 + "s")

    countTotalByHourAndPartyAndBoth( tweetsSinceXV2,false)

    val t4 = System.nanoTime()
    println("Elapsed time Berechnung1: " + (t4 - t3) / 1000000000.0 + "s")

    avgTweetLengthByTimeAndPartyAndBoth( tweetsSinceXV2,false)

    val t5 = System.nanoTime()
    println("Elapsed time Berechnung2: " + (t5 - t4) / 1000000000.0 + "s")
    println("Hello World")
  }

}
