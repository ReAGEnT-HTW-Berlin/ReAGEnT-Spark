import Analysis._
import Utilities._

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document

object Main {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      //.config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.SampleData?authSource=examples")
      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.bson-gaertner?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()


    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd

    val rddWithoutRetweets = rdd.filter(isRetweet)

    countTotalByHourAndParty(rdd)

//    println("Anzahl Retweets: " + rdd.filter(isRetweet).count())
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

    countConnectedHashtags(rddWithoutRetweets)

    println("Hello World")
  }

  //  def countTotalByHourAndParty(rdd: MongoRDD[Document], saveToDB: Boolean = false): Unit = {
  //    val processed =
  //      rdd
  //        .groupBy(tweet => (tweet.get(party), transformTwitterDate(tweet.getString(timestamp)).getHour))
  //        .mapValues(_.size)
  //        .sortBy(elem => (elem._1._2, -elem._2))
  //
  //    println(processed.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))
  //
  //    if (saveToDB) {
  //      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",hour: " + elem._1._2 + "},count: " + elem._2 + "}"))
  //      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.ProcessedTweets?authSource=examples")))
  //    }
  //  }
  //
  //  def countTotalByParty(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .groupBy(_.get(party))
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .collect()
  //        .mkString("Wie viele Tweets pro Partei\n", "\n", "")
  //    )
  //  }
  //
  //  def countTotalByHour(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .groupBy(tweet => transformTwitterDate(tweet.getString(timestamp)).getHour)
  //        .mapValues(_.size)
  //        .sortBy(_._1)
  //        .collect()
  //        .mkString("Wie viele Tweets pro Stunde\n", "\n", "")
  //    )
  //  }
  //
  //  def countByHashtagAndParty(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .flatMap(tweet => getHashtags(tweet.getString(text)).map(hashtag => (hashtag, tweet.get(party))))
  //        .groupBy(identity)
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .take(20)
  //        .mkString("Wie oft nutzt welche Partei welchen Hashtag\n", "\n", "")
  //    )
  //  }
  //
  //  def countByHashtag(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .flatMap(tweet => getHashtags(tweet.getString(text)))
  //        .groupBy(identity)
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .take(10)
  //        .mkString("Wie oft wurde welcher Hashtag genutzt\n", "\n", "")
  //    )
  //  }
  //
  //  def countHashtagsUsedByParty(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .flatMap(tweet => getHashtags(tweet.getString(text)).map(hashtag => (hashtag, tweet.get(party))))
  //        .groupBy(_._2)
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .take(10)
  //        .mkString("Anzahl genutzter Hashtags pro Partei\n", "\n", "")
  //    )
  //  }
  //
  //  def countByNounsAndParty(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .flatMap(tweet => getNouns(tweet.getString(text)).map(noun => (noun, tweet.get(party))))
  //        .groupBy(identity)
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .take(20)
  //        .mkString("Wie oft nutzt welche Partei welche Nomen\n", "\n", "")
  //    )
  //  }
  //
  //  def countBySource(rdd: MongoRDD[Document]): Unit = {
  //    println(
  //      rdd
  //        .groupBy(_.get(source))
  //        .mapValues(_.size)
  //        .sortBy(-_._2)
  //        .collect()
  //        .mkString("Von wo werden wie viele Tweets gepostet\n", "\n", "")
  //    )
  //  }

}
