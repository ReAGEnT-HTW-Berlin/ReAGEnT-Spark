import Utilities._
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.bson.Document

object Analysis {

  def countTotalByHourAndParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .groupBy(tweet => (getParty(tweet),getTime(tweet)))
        .mapValues(_.size)
        .sortBy(elem => (elem._1._2, -elem._2))

    println(processed.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._4 + "},count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countTotalByHourAndParty?authSource=examples")))
    }
  }

  def countTotalByParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .groupBy(getParty)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed .collect().mkString("Wie viele Tweets pro Partei\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countTotalByParty?authSource=examples")))
    }
  }

  def countTotalByHour(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
      .groupBy(tweet => getTime(tweet))
      .mapValues(_.size)
      .sortBy(_._1)

    println(processed.collect().mkString("Wie viele Tweets pro Stunde\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {" +
        "year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._4 +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countTotalByHour?authSource=examples")))
    }
  }

  def countByHashtagAndParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getHashtags(tweet).map(hashtag => (hashtag, getParty(tweet))))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)
    println(processed.take(20).mkString("Wie oft nutzt welche Partei welchen Hashtag\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._2 + "\"" + "hashtag: \"" + elem._1._1 + "\"" +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countByHashtagAndParty?authSource=examples")))
    }
  }

  def countByHashtag(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getHashtags(tweet))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.take(10).mkString("Wie oft wurde welcher Hashtag genutzt\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {hashtag: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countByHashtag?authSource=examples")))
    }
  }

  def countHashtagsUsedByParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getHashtags(tweet).map(hashtag => (hashtag, getParty(tweet))))
        .groupBy(_._2)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.take(10).mkString("Anzahl genutzter Hashtags pro Partei\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countHashtagsUsedByParty?authSource=examples")))
    }
  }

  def countByNounsAndParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getNouns(getText(tweet)).map(noun => (noun, getParty(tweet))))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.take(20).mkString("Wie oft nutzt welche Partei welche Nomen\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._2 + "\"" +
        ",noun: \"" + elem._1._1 + "\"" +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countByNounsAndParty?authSource=examples")))
    }
  }

  def countBySource(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .groupBy(getSource)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.collect().mkString("Von wo werden wie viele Tweets gepostet\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {source: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countBySource?authSource=examples")))
    }
  }

  def countConnectedHashtags(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
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

    println(processed.take(20).mkString("Wie oft werden welche Hashtags zusammen gepostet\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {hashtag1: \"" + elem._1._1 + "\"" +
        ",hashtag2: \"" + elem._1._2 + "\"" +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.countConnectedHashtags?authSource=examples")))
    }
  }

  def avgTweetLengthByParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed = rdd
      .map(elem => (getParty(elem),getText(elem).length))
      .groupBy(_._1)
      .map(elem => (elem._1,(elem._2.reduce((A,B)=> (A._1,A._2+B._2)))._2 / elem._2.size))

    println(processed.collect().mkString("Was ist die Durchschnittslänge der Tweets nach Partein\n", "\n", ""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"" +
        "},length: \"" + elem._2 + "\"" +
        "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.avgTweetLengthByParty?authSource=examples")))
    }
  }

  def avgTweetLengthByTime(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed = rdd
      .map(elem => (getTime(elem),getText(elem).length))
      .groupBy(_._1)
      .map(elem => (elem._1,(elem._2.reduce((A,B)=> (A._1,A._2+B._2)))._2 / elem._2.size))

    println(processed.collect().mkString("Durchschnittslänge der Tweets nach Zeit \n","\n",""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {" +
        "year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._4 + "},length: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.avgTweetLengthByTime?authSource=examples")))
    }
  }

  def avgTweetLengthByTimeAndParty(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed = rdd
      .map(elem => ((getParty(elem), getTime(elem)),getText(elem).length))
      .groupBy(_._1)
      .map(elem => (elem._1,(elem._2.reduce((A,B)=> (A._1,A._2+B._2)))._2 / elem._2.size))

    println(processed.collect().mkString("Durchschnittslänge der Tweets nach Zeit und Partei \n","\n",""))

    if(saveToDB){
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._4 + "},length: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.avgTweetLengthByTimeAndParty?authSource=examples")))
    }
  }


  //Textlaenge
  //Reply_settings

}
