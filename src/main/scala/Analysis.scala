import Utilities._
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.bson.Document

object Analysis {

  def countTotal(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll =
      rdd
        .groupBy(tweet => (getParty(tweet), getTime(tweet)))
        .mapValues(_.size)
        .sortBy(elem => (elem._1._2, -elem._2))
    println(processedAll.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    val processedParty = processedAll.groupBy(_._1._1).mapValues(_.map(_._2).sum).sortBy(-_._2)
    println(processedParty.collect().mkString("Wie viele Tweets pro Partei\n", "\n", ""))

    val processedHour = processedAll.groupBy(_._1._2).mapValues(_.map(_._2).sum).sortBy(_._1)
    println(processedHour.collect().mkString("Wie viele Tweets pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    println(processedYearAndParty.collect().mkString("Wie viele Tweets pro Jahr pro Partei\n", "\n", ""))


    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    println(processedMonthAndParty.collect().mkString("Wie viele Tweets pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    println(processedWeekAndParty.collect().mkString("Wie viele Tweets pro Woche pro Partei\n", "\n", ""))


    if (saveToDB) {
      /*val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 + "},count: " + elem._2 + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByHourAndParty?authSource=examples"))))

      val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"},count: " + elem._2 + "}"))
      docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByParty?authSource=examples"))))

      val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
        ",year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._5 + "},count: " + elem._2 + "}"))
      docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByHour?authSource=examples"))))*/

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countTotalByWeek?authSource=examples"))))
    }
  }

  def countByHashtag(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getHashtags(tweet).map(hashtag => (hashtag, getParty(tweet))))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)
    println(processed.take(20).mkString("Wie oft nutzt welche Partei welchen Hashtag\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._2 + "\"" + "hashtag: \"" + elem._1._1 + "\"" +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countByHashtagAndParty?authSource=examples"))))
    }
  }

  /*def countByHashtag(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .flatMap(tweet => getHashtags(tweet))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.take(10).mkString("Wie oft wurde welcher Hashtag genutzt\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {hashtag: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countByHashtag?authSource=examples"))))
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

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countHashtagsUsedByParty?authSource=examples"))))
    }
  }*/

  def countByURL(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed =
      rdd
        .groupBy(getSource)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.collect().mkString("Von wo werden wie viele Tweets gepostet\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {source: \"" + elem._1 + "\"" + "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countBySource?authSource=examples"))))
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
            (for (i <- 0 until hashtags.size - 1) yield for (j <- i + 1 until hashtags.size) yield (hashtags(i), hashtags(j))).flatten
          }
        })
        .filter(tuple => tuple._1 != tuple._2)
        .groupBy(tuple => if (tuple._1 < tuple._2) tuple else tuple.swap)
        .mapValues(_.size)
        .sortBy(-_._2)

    println(processed.take(20).mkString("Wie oft werden welche Hashtags zusammen gepostet\n", "\n", ""))

    if (saveToDB) {
      val docs = processed.map(elem => Document.parse("{_id: {hashtag1: \"" + elem._1._1 + "\"" +
        ",hashtag2: \"" + elem._1._2 + "\"" +
        "}, count: " + elem._2 + "}"))
      docs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.countConnectedHashtags?authSource=examples"))))
    }
  }

  /**
   * /averageTweets -> [{"CDU": {"2020": 123, ...}}, {"SPD": {"2017": 5, ...}}, ...] -> wie count
   */
  def avgTweetLength(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processed = rdd
      .map(elem => ((getParty(elem), getTime(elem)), getText(elem).length))
      .groupBy(_._1)
      .map(elem => (elem._1, (elem._2.reduce((A, B) => (A._1, A._2 + B._2)))._2 / elem._2.size))

    println(processed.collect().mkString("Durchschnittslänge der Tweets nach Zeit und Partei \n", "\n", ""))

    val processedByParty = processed.groupBy(_._1._1).mapValues(x => x.map(_._2).sum / x.size)

    println(processedByParty.collect().mkString("Durchschnittslänge der Tweets nach Partei \n", "\n", ""))

    val processedByTime = processed.groupBy(_._1._2).mapValues(x => x.map(_._2).sum / x.size)

    println(processedByTime.collect().mkString("Durchschnittslänge der Tweets nach Zeit \n", "\n", ""))

    if (saveToDB) {
      val processedDocs = processed.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._4 + "},length: " + elem._2 + "}"))
      processedDocs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByTimeAndParty?authSource=examples"))))

      val processedByPartyDocs = processedByParty.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"" +
        "},length: \"" + elem._2 + "\"" +
        "}"))
      processedByPartyDocs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByParty?authSource=examples"))))

      val processedByTimeDocs = processedByTime.map(elem => Document.parse("{_id: {" +
        "year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._4 + "},length: " + elem._2 + "}"))
      processedByTimeDocs.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByTime?authSource=examples"))))
    }

  }

  /**
   * /averageReply -> [{"CDU": {"2020": 1, ...}}, {"SPD": {"2017": 0.25, ...}}, ...] -> wie count
   */
  def avgReplies(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /mosttweetsday -> [{"CDU": {"Mittwoch": 7642, ...}}, {"SPD": {"Donnerstag": 6234, ...}}, ...] -> wie count
   */
  def mostTweetsDay(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /mosttweetstime -> [{"CDU": {"01": 7642, ...}}, {"SPD": {"17": 6234, ...}}, ...] -> wie count
   */
  def mostTweetsTime(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /averagelikestweet -> [{"CDU": {"2020": 123, ...}}, {"SPD": {"2017": 5, ...}}, ...] -> wie count
   */
  def avgLikes(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /averageanswerstweet -> [{"CDU": {"2020": 123, ...}}, {"SPD": {"2017": 5, ...}}, ...] -> wie count
   */
  def avgAnswers(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /mediausagetweets -> [{"CDU": {"2020": 0.05, ...}}, {"SPD": {"2017": 0.5, ...}}, ...] -> wie count
   */
  def mediaUsage(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /mostTagedUser -> {"2020": {"1": "User1", ... "10": "User10"}, ...}
   */
  def mostTaggedUsers(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???

  /**
   * /mostActiveUser -> {"2020": {"1": "User1", ... "5": "User5"}, ...}
   */
  def mostActiveUsers(rdd: RDD[Document], saveToDB: Boolean = false): Unit = ???



}
