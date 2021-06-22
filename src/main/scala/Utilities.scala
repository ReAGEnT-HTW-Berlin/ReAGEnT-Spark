import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.bson.Document

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object Utilities {
  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSX")
  val dtf_new: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

  def getAllWithRegex(text: String, pattern: Regex): List[String] = {
    pattern.findAllIn(text).toList
  }

  def getNouns(text: String): List[String] = {
    getAllWithRegex(text, "[A-ZÄÖÜẞ][\\w\\xc0-\\xff]{4,}".r)
  }

  def isRetweet(document: Document): Boolean = {
    document.getBoolean("retweet")
  }

  //def transformTwitterDate(date: String): LocalDateTime = {
  //  val formatted = date.replace('T', ' ')
  //  LocalDateTime.parse(formatted, dtf).plusHours(2)
  //}
  def transformTwitterDate(date: String): LocalDateTime = LocalDateTime.parse(date.splitAt(19)._1, dtf_new).plusHours(2)


  //Methoden für Zugriff auf BSON Tweet-Darstellung
  //def getText(document: Document): String = document.get("data").asInstanceOf[Document].getString("text")
  def getText(document: Document): String = document.getString("tweet")

  //def getParty(document: Document): String = document.get("matching_rules").asInstanceOf[util.ArrayList[Document]].asScala.head.getString("tag")
  def getParty(document: Document): String = document.getString("partei")

  //def getTimestamp(document: Document): LocalDateTime = transformTwitterDate(document.get("data").asInstanceOf[Document].getString("created_at"))
  def getTimestamp(document: Document): LocalDateTime = transformTwitterDate(document.getString("created_at"))

  def getSource(document: Document): String = document.get("data").asInstanceOf[Document].getString("source")

//  def getHashtags(document: Document): List[String] = {
//    if (document.get("data").asInstanceOf[Document].get("entities") == null || document.get("data").asInstanceOf[Document].get("entities").asInstanceOf[Document].get("hashtags") == null)
//      List()
//    else
//      document
//        .get("data").asInstanceOf[Document]
//        .get("entities").asInstanceOf[Document]
//        .get("hashtags").asInstanceOf[util.ArrayList[Document]].asScala
//        .map(_.getString("tag")).toList
//  }
  def getHashtags(document: Document): List[String] = {
    document
      .get("hashtags").asInstanceOf[util.ArrayList[String]].asScala.toList
  }

  def getTime(document: Document): (Int, Int, Int, Int, Int, String) = {
    val timeStamp = getTimestamp(document);
    (timeStamp.getYear, timeStamp.getMonth.getValue, timeStamp.getDayOfMonth,timeStamp.getDayOfYear, timeStamp.getHour, timeStamp.getDayOfWeek.name())
  }

  def getRepliesCount(document: Document): Int = document.getInteger("replies_count")

  def getLikesCount(document: Document): Int = document.getInteger("likes_count")

  // Tims Block //////////////////////////////

  def getMediaCount(document: Document): Int = document.getInteger("video")+document.get("photos").asInstanceOf[util.ArrayList[String]].size()

  def getUser(document:Document): String = document.getString("username")

  def getTaggedUserList(document: Document):List[String] = {
    document.get("mentions").asInstanceOf[util.ArrayList[Document]].asScala
      .map(_.getString("screen_name")).toList
  }

  //End Block////////////////////////////////

  // Saschas Block //////////////////////////

  //End Block /////////////////////////////

  def saveTweetsFromThisPeriod(rdd: RDD[Document]): Unit = {
    val after2017 = rdd
      .filter(
        tweet => getTimestamp(tweet)
          .isAfter(LocalDateTime.parse("2017-09-24 23:59:59", DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")))
      )
    after2017.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets_bundestag_aktuelle_legislaturperiode?authSource=examples")))
  }
}
