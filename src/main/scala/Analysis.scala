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
    //println(processedAll.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    //val processedParty = processedAll.groupBy(_._1._1).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedParty.collect().mkString("Wie viele Tweets pro Partei\n", "\n", ""))

    //val processedHour = processedAll.groupBy(_._1._2).mapValues(_.map(_._2).sum).sortBy(_._1)
    //println(processedHour.collect().mkString("Wie viele Tweets pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    //println(processedYearAndParty.collect().mkString("Wie viele Tweets pro Jahr pro Partei\n", "\n", ""))


    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    //println(processedMonthAndParty.collect().mkString("Wie viele Tweets pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(_.map(_._2).sum).sortBy(elem => (elem._1._2, -elem._2))
    //println(processedWeekAndParty.collect().mkString("Wie viele Tweets pro Woche pro Partei\n", "\n", ""))


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
    val processedAll =
      rdd.flatMap(tweet => getHashtags(tweet).map(hashtag => (getParty(tweet), getTime(tweet), hashtag)))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    val minTweets = 10

    //val processedParty = processedAll.groupBy(x => (x._1._1, x._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)
    //println(processedParty.collect().take(25).mkString("Anzahl Hastags  pro Partei\n", "\n", ""))

    //val processedHour = processedAll.groupBy(x => (x._1._2, x._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)
    //println(processedHour.collect().take(25).mkString("Anzahl Hastags pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)
    //println(processedYearAndParty.collect().take(25).mkString("Anzahl Hastags pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)
    //println(processedMonthAndParty.collect().take(25).mkString("Anzahl Hastags pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTweets)
    //println(processedWeekAndParty.collect().take(25).mkString("Anzahl Hastags pro Woche pro Partei\n", "\n", ""))

    if (saveToDB) {
      /*val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 +
        ",hashtag: \""+elem._1._3 +
        "\"}," +
        "count: " + elem._2 + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByHourAndParty?authSource=examples"))))*/

      /* val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\", hashtag: \""+ elem._1._2 +"\" },count: " + elem._2 + "}"))
       docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByParty?authSource=examples"))))

       val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
         ",year: " + elem._1._1._1 +
         ",month: " + elem._1._1._2 +
         ",day: " + elem._1._1._3 +
         ",hour: " + elem._1._1._5 +
         ",hashtag: \""+elem._1._2 +
         "\"},count: " + elem._2 + "}"))
       docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.hashtagsByHour?authSource=examples"))))*/

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

    ////println(processed.collect().mkString("Durchschnittslänge der Tweets nach Zeit und Partei \n", "\n", ""))

    //val processedByParty = processedAll.groupBy(_._1._1).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)
    //println(processedByParty.collect().mkString("Durchschnittslänge der Tweets nach Partei \n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Durchschnittslänge der Tweets pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Durchschnittslänge der Tweets pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2).sum / x.size).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Durchschnittslänge der Tweets pro Woche pro Partei\n", "\n", ""))

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

    ////println(processedAll.collect().mkString("Array(", ", ", ")")) zu gross

    //val processedParty = processedAll.groupBy(x => x._1._1).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedParty.collect().mkString("Durchschnittliche Antwortanzahl pro Partei\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Durchschnittliche Antwortanzahl pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Durchschnittliche Antwortanzahl pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Durchschnittliche Antwortanzahl pro Woche pro Partei\n", "\n", ""))

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
    //println(processedAll.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    //val processedParty = processedAll
    //  .groupBy(elem => (elem._1._1, elem._1._2._6))
    //  .mapValues(_.map(_._2).sum)
    //println(processedParty.collect().mkString("Meisten Tweets an einen Wochentag pro Partei\n", "\n", ""))

    val processedYearAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._6))
      .mapValues(_.map(_._2).sum)
    //println(processedYearAndParty.collect().mkString("Meisten Tweets an einen Wochentag pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._2._6))
      .mapValues(_.map(_._2).sum)
    //println(processedMonthAndParty.collect().mkString("Meisten Tweets an einen Wochentag pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._2._6))
      .mapValues(_.map(_._2).sum)
    //println(processedWeekAndParty.collect().mkString("Meisten Tweets an einen Wochentag pro Woche pro Partei\n", "\n", ""))

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
    //println(processedAll.collect().mkString("Wie viele Tweets pro Partei pro Stunde\n", "\n", ""))

    //val processedParty = processedAll
    //  .groupBy(elem => (elem._1._1, elem._1._2._5))
    //  .mapValues(_.map(_._2).sum)
    //println(processedParty.collect().mkString("Meisten Tweets zu einer Tageszeit pro Partei\n", "\n", ""))

    val processedYearAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._5))
      .mapValues(_.map(_._2).sum)
    //println(processedYearAndParty.collect().mkString("Meisten Tweets zu einer Tageszeit pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._2._5))
      .mapValues(_.map(_._2).sum)
    //println(processedMonthAndParty.collect().mkString("Meisten Tweets zu einer Tageszeit pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll
      .groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._2._5))
      .mapValues(_.map(_._2).sum)
    //println(processedWeekAndParty.collect().mkString("Meisten Tweets zu einer Tageszeit pro Woche pro Partei\n", "\n", ""))

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

    ////println(processedAll.collect().mkString("Array(", ", ", ")")) zu gross
    //val processedParty = processedAll.groupBy(x => x._1._1).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedParty.collect().mkString("Durchschnittliche Likeanzahl pro Partei\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Durchschnittliche Likeanzahl pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Durchschnittliche Likeanzahl pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Durchschnittliche Likeanzahl pro Woche pro Partei\n", "\n", ""))

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

    /*val processedParty = processedAll.groupBy(x => x._1._1).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)
    //println(processedParty.collect().mkString("Durchschnittliche Medienanzahl pro Partei\n", "\n", ""))

    val processedHour = processedAll.groupBy(_._1._2).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)
    //println(processedHour.collect().mkString("Durchschnittliche Medienanzahl pro Stunde\n", "\n", ""))*/

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Durchschnittliche Medienanzahl pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Durchschnittliche Medienanzahl pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Durchschnittliche Medienanzahl pro Woche pro Partei\n", "\n", ""))

    if (saveToDB) {

      val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 + "}," +
        "count: " + elem._2.head + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByHourAndParty?authSource=examples"))))

      /*val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"},count: " + elem._2 + "}"))
      docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByParty?authSource=examples"))))

      val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
        ",year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._5 + "},count: " + elem._2 + "}"))
      docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mediaUsageByHour?authSource=examples"))))*/

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

    //val processedParty = processedAll.groupBy(x => (x._1._1, x._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)
    //println(processedParty.collect().take(25).mkString("Meistgetaggte User  pro Partei\n", "\n", ""))

    //val processedHour = processedAll.groupBy(x => (x._1._2, x._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)
    //println(processedHour.collect().take(25).mkString("Meistgetaggte User pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)
    //println(processedYearAndParty.collect().take(25).mkString("Meistgetaggte User pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)
    //println(processedMonthAndParty.collect().take(25).mkString("Meistgetaggte User pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minTaggs)
    //println(processedWeekAndParty.collect().take(25).mkString("Meistgetaggte User pro Woche pro Partei\n", "\n", ""))

    if (saveToDB) {
      /*val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 +
        ",taggedUser: \""+elem._1._3 +
        "\"}," +
        "count: " + elem._2 + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUserssByHourAndParty?authSource=examples"))))*/

      /* val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\", taggedUser: \""+ elem._1._2 +"\" },count: " + elem._2 + "}"))
       docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUsersByParty?authSource=examples"))))

       val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
         ",year: " + elem._1._1._1 +
         ",month: " + elem._1._1._2 +
         ",day: " + elem._1._1._3 +
         ",hour: " + elem._1._1._5 +
         ",taggedUser: \""+elem._1._2 +
         "\"},count: " + elem._2 + "}"))
       docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTaggedUsersByHour?authSource=examples"))))*/

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

    val minTweets = 10

    //val processedParty = processedAll.groupBy(x => (x._1._1, x._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)
    //println(processedParty.collect().take(minTweets).mkString("Aktivste Nutzer  pro Partei\n", "\n", ""))

    //val processedHour = processedAll.groupBy(x => (x._1._2, x._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)
    //println(processedHour.collect().take(minTweets).mkString("Aktivste Nutzer pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)
    //println(processedYearAndParty.collect().take(minTweets).mkString("Aktivste Nutzer pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)
    //println(processedMonthAndParty.collect().take(minTweets).mkString("Aktivste Nutzer pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2.sum).sum).sortBy(-_._2)
    //println(processedWeekAndParty.collect().take(minTweets).mkString("Aktivste Nutzer pro Woche pro Partei\n", "\n", ""))

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

      /*val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\", user: \"" + elem._1._2 + "\" },count: " + elem._2 + "}"))
      docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByParty?authSource=examples"))))

      val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
        ",year: " + elem._1._1._1 +
        ",month: " + elem._1._1._2 +
        ",day: " + elem._1._1._3 +
        ",hour: " + elem._1._1._5 +
        ",user: \"" + elem._1._2 +
        "\"},count: " + elem._2 + "}"))
      docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostActiveUsersByHour?authSource=examples"))))*/

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

    //val processedParty = processedAll.groupBy(_._1._1).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedParty.collect().mkString("Anzahl Replies pro Partei\n", "\n", ""))

    //val processedHour = processedAll.groupBy(_._1._2).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedHour.collect().mkString("Anzahl Replies pro Stunde\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Anzahl Replies pro Jahr pro Partei\n", "\n", ""))


    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Anzahl Replies pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(_.map(_._2).sum).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Anzahl Repliess pro Woche pro Partei\n", "\n", ""))

    if (saveToDB) {
      /*val docsAll = processedAll.map(elem => Document.parse("{_id: {party: \"" + elem._1._1 + "\"" +
        ",year: " + elem._1._2._1 +
        ",month: " + elem._1._2._2 +
        ",day: " + elem._1._2._3 +
        ",hour: " + elem._1._2._5 + "},count: " + elem._2 + "}"))
      docsAll.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByHourAndParty?authSource=examples"))))

      val docsParty = processedParty.map(elem => Document.parse("{_id: {party: \"" + elem._1 + "\"},count: " + elem._2 + "}"))
      docsParty.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByParty?authSource=examples"))))

      val docsHour = processedHour.map(elem => Document.parse("{_id: {" +
        ",year: " + elem._1._1 +
        ",month: " + elem._1._2 +
        ",day: " + elem._1._3 +
        ",hour: " + elem._1._5 + "},count: " + elem._2 + "}"))
      docsHour.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.totalRepliesByHour?authSource=examples"))))*/

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

    //val processedParty = processedAll.groupBy(x => x._1._1).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedParty.collect().mkString("Durchschnittliche Retweetanzahl pro Partei\n", "\n", ""))

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedYearAndParty.collect().mkString("Durchschnittliche Retweetanzahl pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedMonthAndParty.collect().mkString("Durchschnittliche Retweetanzahl pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1)).mapValues(x => x.map(_._2.sum).sum / x.map(_._2.size).sum.toDouble).sortBy(-_._2)
    //println(processedWeekAndParty.collect().mkString("Durchschnittliche Retweetanzahl pro Woche pro Partei\n", "\n", ""))

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

  def countUrls(rdd: RDD[Document], saveToDB: Boolean = false):Unit = {
    val processedAll =
      rdd.flatMap(tweet => getUrls(tweet).map(url => (getParty(tweet), getTime(tweet), url)))
        .groupBy(identity)
        .mapValues(_.size)
        .sortBy(-_._2)

    val minUrls = 1

    val processedYearAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)
    //println(processedYearAndParty.collect().take(25).mkString("Anzahl Urls pro Jahr pro Partei\n", "\n", ""))

    val processedMonthAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._2, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)
    //println(processedMonthAndParty.collect().take(25).mkString("Anzahl Urls pro Monat pro Partei\n", "\n", ""))

    val processedWeekAndParty = processedAll.groupBy(elem => (elem._1._1, elem._1._2._1, elem._1._2._4 / 7 + 1, elem._1._3)).mapValues(x => x.map(_._2).sum).sortBy(-_._2).filter(_._2 > minUrls)
    //println(processedWeekAndParty.collect().take(25).mkString("Anzahl Urls pro Woche pro Partei\n", "\n", ""))

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
