import Utilities.{getHashtags, getNouns, getParty, getSource, getText, getTimestamp}
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.bson.Document

object Analysis {

  def countTotalByHourAndParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .groupBy(tweet => (getParty(tweet), getTimestamp(tweet).getHour))
        .mapValues(_.size)
        .sortBy(elem => (elem._1._2, -elem._2))

    println(processed.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",hour: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.ProcessedTweets?authSource=examples")))
    }
  }

  def countTotalByParty(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .groupBy(getParty)
        .mapValues(_.size)
        .sortBy(-_._2)
        .collect()
        .mkString("Wie viele Tweets pro Partei\n", "\n", "")
    )
  }

  def countTotalByHour(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .groupBy(tweet => getTimestamp(tweet).getHour)
        .mapValues(_.size)
        .sortBy(_._1)
        .collect()
        .mkString("Wie viele Tweets pro Stunde\n", "\n", "")
    )
  }

  def countByHashtagAndParty(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .flatMap(tweet => getHashtags(tweet).map(hashtag => (hashtag, getParty(tweet))))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)
        .take(20)
        .mkString("Wie oft nutzt welche Partei welchen Hashtag\n", "\n", "")
    )
  }

  def countByHashtag(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .flatMap(tweet => getHashtags(tweet))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)
        .take(10)
        .mkString("Wie oft wurde welcher Hashtag genutzt\n", "\n", "")
    )
  }

  def countHashtagsUsedByParty(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .flatMap(tweet => getHashtags(tweet).map(hashtag => (hashtag, getParty(tweet))))
        .groupBy(_._2)
        .mapValues(_.size)
        .sortBy(-_._2)
        .take(10)
        .mkString("Anzahl genutzter Hashtags pro Partei\n", "\n", "")
    )
  }

  def countByNounsAndParty(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .flatMap(tweet => getNouns(getText(tweet)).map(noun => (noun, getParty(tweet))))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)
        .take(20)
        .mkString("Wie oft nutzt welche Partei welche Nomen\n", "\n", "")
    )
  }

  def countBySource(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .groupBy(getSource)
        .mapValues(_.size)
        .sortBy(-_._2)
        .collect()
        .mkString("Von wo werden wie viele Tweets gepostet\n", "\n", "")
    )
  }

  def countConnectedHashtags(rdd: RDD[Document]): Unit = {
    println(
      rdd
        .flatMap(tweet => {
          val hashtags = getHashtags(tweet)
          if (hashtags.size < 2) {
            List()
          } else {
            (for (i <- 0 until hashtags.size - 1) yield for (j <- i+1 until hashtags.size) yield (hashtags(i), hashtags(j))).flatten
          }
        })
        .filter(tuple => tuple._1 != tuple._2)
        .groupBy(tuple => if (tuple._1 < tuple._2) tuple else tuple.swap)
        .mapValues(_.size)
        .sortBy(-_._2)
        .take(20)
        .mkString("Wie oft werden welche Hashtags zusammen gepostet\n", "\n", "")
    )
  }

}
