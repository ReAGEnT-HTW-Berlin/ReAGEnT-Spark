import org.bson.Document

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object Utilities {
  val dtf_new: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

  def getAllWithRegex(text: String, pattern: Regex): List[String] = {
    pattern.findAllIn(text).toList
  }

  def isRetweet(document: Document): Boolean = {
    document.getBoolean("retweet")
  }

  /**
   * Wandelt den String-Timestamp von Twitter in ein LocalDateTime Object um
   * @param date Date Feld vom Tweet
   * @return LocalDateTime, vom angegeben Zeitpunkt
   */
  def transformTwitterDate(date: String): LocalDateTime = LocalDateTime.parse(date.splitAt(19)._1, dtf_new).plusHours(2)

  //Methoden für Zugriff auf BSON Tweet-Darstellung

  /**
   * Erfragt vom Tweet json-Objekt, was der Inhalt des Tweets ist
   * @param document Tweet
   * @return String, Tweet-Text
   */
  def getText(document: Document): String = document.getString("tweet")

  def getParty(document: Document): String = document.getString("partei")

  def getTimestamp(document: Document): LocalDateTime = transformTwitterDate(document.getString("created_at"))

  def getSource(document: Document): String = document.get("data").asInstanceOf[Document].getString("source")

  def getHashtags(document: Document): List[String] = {
    document
      .get("hashtags").asInstanceOf[util.ArrayList[String]].asScala.toList
  }

  def getTime(document: Document): (Int, Int, Int, Int, Int, String) = {
    val timeStamp = getTimestamp(document)
    (timeStamp.getYear, timeStamp.getMonth.getValue, timeStamp.getDayOfMonth,timeStamp.getDayOfYear, timeStamp.getHour, timeStamp.getDayOfWeek.name())
  }

  /**
   * Erfragt vom Tweet json-Objekt, wie viele Antworten der Tweet erhalten hat
   * @param document Tweet
   * @return Int, Anzahl Antworten
   */
  def getRepliesCount(document: Document): Int = document.getInteger("replies_count")

  def getLikesCount(document: Document): Int = document.getInteger("likes_count")

  def getMediaCount(document: Document): Int = document.getInteger("video")

  /**
   * Erfragt vom Tweet json-Objekt, wer den Tweet verfasst hat
   * @param document Tweet
   * @return String, Username vom Author
   */
  def getUser(document:Document): String = document.getString("username")

  def getTaggedUserList(document: Document):List[String] = {
    document.get("mentions").asInstanceOf[util.ArrayList[Document]].asScala
      .map(_.getString("screen_name")).toList
  }

  def getRetweetsCount(document:Document): Int = document.getInteger("retweets_count")

  /**
   * Erfragt vom Tweet json-Objekt, welche URLs verwendet wurden
   * @param document Tweet
   * @return List[String] an URLs
   */
  def getUrls(document:Document): List[String] = {
    document
      .get("urls")
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .map(url => url.split("//")(1).split("/")(0))
      .toList
      .map(mergeSameUrls)
  }

  /**
   * Vereinheitlicht URLs, die auf die selbe Seite weiterleiten
   * @param str Urspruengliche URL
   * @return Umgewandelte URL
   */
  def mergeSameUrls(str: String): String = {
    str match {
      case "youtu.be" => "www.youtube.com"
      case "fb.me" => "www.facebook.com"
      case "www.google.com" => "www.google.de"
      case "goo.gl" => "www.google.de"
      case _ => str
    }
  }
}
