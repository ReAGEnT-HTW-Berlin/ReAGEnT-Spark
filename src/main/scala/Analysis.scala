import Utilities._
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.bson.Document

object Analysis {

  /**
   * Das Prinzip der nachfolgenden Methoden folgt der selben Logik wie dieser
   *
   * Grundsaetzlich werden eine Menge von Tweets genommen und Operationen wie das Zaehlen darauf ausgefuehrt
   * @param rdd, Resilient Distributed Dataset, Spark format in den sich die Tweets befinden
   * @param saveToDB, Boolean ob Berechnungen/Erkenntnisse in die Datenbank geschrieben werden sollen
   */
  def countTotal(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    //Zunaechst werden die Daten gruppiert nach Partei und Zeitstempel. Danach wird das eigentliche Ergebnis berechnet
    val processedAll =
      rdd
        .groupBy(tweet => (getParty(tweet), getTime(tweet)))
        .mapValues(_.size)
        .sortBy(elem => (elem._1._2, -elem._2))

    //Die berechneten Ergebnisse werden nun in 3 verschiedene Gruppen gruppiert um in die Datenbank geschrieben zu werden
    // 1 - gruppiert Nach Jahr und Partei
    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    // 2 - gruppiert Nach Monat und Partei
    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    // 3 - gruppiert Nach Woche und Partei
    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))


    if (saveToDB) {

      //Speichert die Daten in die Datenbank. Dabei werden Sie zu JSONs umgewandelt
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
    val processedAll =
      rdd.flatMap(tweet => getHashtags(tweet).map(hashtag => (getParty(tweet), getTime(tweet), hashtag)))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    val minTweets = 10

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + ", hashtag: \"" + elem._1._3 + "\" },count: " + elem._2.asInstanceOf[Double] + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",hashtag: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",hashtag: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByWeek?authSource=examples"))))
    }

  }

  /**
   * /averageTweets -> [{"CDU": {"2020": 123, ...}}, {"SPD": {"2017": 5, ...}}, ...] -> wie count
   */
  def avgTweetLength(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .map(elem => ((getParty(elem), getTime(elem)), getText(elem).length))
      .groupBy(_._1)
      .map(elem => (elem._1, (elem._2.reduce((A, B) => (A._1, A._2 + B._2)))._2 / elem._2.size))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)

    if (saveToDB) {
      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgTweetLengthByWeek?authSource=examples"))))
    }
  }

  /**
   * /averageReply -> [{"CDU": {"2020": 1, ...}}, {"SPD": {"2017": 0.25, ...}}, ...] -> wie count
   */
  def avgReplies(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(tweets => tweets.map(tweet => getRepliesCount(tweet)))
      .sortBy(elem => (elem._1._1)) //nur fuer print

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRepliesByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRepliesByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRepliesByWeek?authSource=examples"))))

    }
  }

  /**
   * /mosttweetsday -> [{"CDU": {"Mittwoch": 7642, ...}}, {"SPD": {"Donnerstag": 6234, ...}}, ...] -> wie count
   */
  def mostTweetsDay(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(_.size)
      .sortBy(elem => (elem._1._2, -elem._2))

    val processedYearAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._6))
      .mapValues(_.map(_._2).sum)

    val processedMonthAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._2._6))
      .mapValues(_.map(_._2).sum)

    val processedWeekAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._2._6))
      .mapValues(_.map(_._2).sum)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse(
        "{_id: " +
          "{party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",weekday: \"" + elem._1._3 +
          "\"},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsDayByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",weekday: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsDayByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",weekday: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsDayByWeek?authSource=examples"))))
    }
  }

  /**
   * /mosttweetstime -> [{"CDU": {"01": 7642, ...}}, {"SPD": {"17": 6234, ...}}, ...] -> wie count
   */
  def mostTweetsTime(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(_.size)
      .sortBy(elem => (elem._1._2, -elem._2))

    val processedYearAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._5))
      .mapValues(_.map(_._2).sum)

    val processedMonthAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._2._5))
      .mapValues(_.map(_._2).sum)

    val processedWeekAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._2._5))
      .mapValues(_.map(_._2).sum)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse(
        "{_id: " +
          "{party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",hour: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsTimeByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",hour: " + elem._1._4 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsTimeByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",hour: " + elem._1._4 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsTimeByWeek?authSource=examples"))))
    }
  }

  /**
   * /averagelikestweet -> [{"CDU": {"2020": 123, ...}}, {"SPD": {"2017": 5, ...}}, ...] -> wie count
   */
  def avgLikes(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(tweets => tweets.map(tweet => getLikesCount(tweet)))
      .sortBy(elem => (elem._1._1)) //nur fuer print

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgLikesByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgLikesByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgLikesByWeek?authSource=examples"))))
    }
  }

  /**
   * /mediausagetweets -> [{"CDU": {"2020": 0.05, ...}}, {"SPD": {"2017": 0.5, ...}}, ...] -> wie count
   */
  def mediaUsage(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(tweets => tweets.map(tweet => getMediaCount(tweet).asInstanceOf[Double]))
      .sortBy(elem => (elem._1._1))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)

    if (saveToDB) {

      val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 + "}," +
        "count: " + elem._2.head + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByHourAndParty?authSource=examples"))))

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2.asInstanceOf[Double] + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByWeek?authSource=examples"))))
    }
  }

  /**
   * /mostTagedUser -> {"2020": {"1": "User1", ... "10": "User10"}, ...}
   */
  def mostTaggedUsers(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .flatMap(tweet => getTaggedUserList(tweet).map(user => (getParty(tweet), getTime(tweet), user)))
      .groupBy(identity)
      .mapValues(_.size)
      .sortBy(-_._2)

    val minTaggs = 2

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + ",taggedUser: \"" + elem._1._3 + "\" },count: " + elem._2.asInstanceOf[Double] + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUsersByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",taggedUser: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUsersByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",taggedUser: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUsersByWeek?authSource=examples"))))
    }
  }

  /**
   * /mostActiveUser -> {"2020": {"1": "User1", ... "5": "User5"}, ...}
   */
  def mostActiveUsers(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet), getUser(tweet)))
      .mapValues(tweets => tweets.map(tweet => getUser(tweet)))
      .groupBy(_._1).mapValues(_.map(_._2.size))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)

    if (saveToDB) {
      val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 +
        ",user: \"" + elem._1._3 +
        "\"}," +
        "count: " + elem._2.head + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByHourAndParty?authSource=examples"))))

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + ", user: \"" + elem._1._3 + "\" },count: " + elem._2.asInstanceOf[Double] + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",user: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",user: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByWeek?authSource=examples"))))

    }

  }

  def totalReplies(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(tweets => tweets.map(tweet => getRepliesCount(tweet).asInstanceOf[Int]).sum)
      .sortBy(elem => (elem._1._1))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(_.map(_._2).sum).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(_.map(_._2).sum).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(_.map(_._2).sum).sortBy(-_._2)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByWeek?authSource=examples"))))
    }
  }

  def avgRetweets(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll = rdd
      .groupBy(tweet => (getParty(tweet), getTime(tweet)))
      .mapValues(tweets => tweets.map(tweet => getRetweetsCount(tweet)))
      .sortBy(elem => (elem._1._1))


    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)

    if (saveToDB) {

      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + "},count: " + elem._2 + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRetweetsByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRetweetsByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          "},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.avgRetweetsByWeek?authSource=examples"))))
    }
  }

  def countUrls(rdd: RDD[Document], saveToDB: Boolean = false): Unit = {
    val processedAll =
      rdd.flatMap(tweet => getUrls(tweet).map(url => (getParty(tweet), getTime(tweet), url)))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    val minUrls = 1

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)

    if (saveToDB) {
      val docsYearAndParty = processedYearAndParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\",year: " + elem._1._2 + ", url: \"" + elem._1._3 + "\" },count: " + elem._2.asInstanceOf[Double] + "}"))
      docsYearAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.urlsByYear?authSource=examples"))))

      val docsMonthAndParty = processedMonthAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",month: " + elem._1._3 +
          ",url: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsMonthAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.urlsByMonth?authSource=examples"))))

      val docsWeekAndParty = processedWeekAndParty.map(elem => Document.parse(
        "{_id: {" +
          "party: \"" + elem._1._1 +
          "\",year: " + elem._1._2 +
          ",week: " + elem._1._3 +
          ",url: \"" + elem._1._4 +
          "\"},count: " + elem._2 + "}"))
      docsWeekAndParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.urlsByWeek?authSource=examples"))))
    }

  }

}
