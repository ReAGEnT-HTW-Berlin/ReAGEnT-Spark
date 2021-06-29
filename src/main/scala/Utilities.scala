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

  def transformTwitterDate(date: String): LocalDateTime = LocalDateTime.parse(date.splitAt(19)._1, dtf_new).plusHours(2)

  //Methoden für Zugriff auf BSON Tweet-Darstellung
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

  def getRepliesCount(document: Document): Int = document.getInteger("replies_count")

  def getLikesCount(document: Document): Int = document.getInteger("likes_count")

  def getMediaCount(document: Document): Int = document.getInteger("video")+document.get("photos").asInstanceOf[util.ArrayList[String]].size()

  def getUser(document:Document): String = document.getString("username")

  def getTaggedUserList(document: Document):List[String] = {
    document.get("mentions").asInstanceOf[util.ArrayList[Document]].asScala
      .map(_.getString("screen_name")).toList
  }

  def getRetweetsCount(document:Document): Int = document.getInteger("retweets_count")

  def getUrls(document:Document): List[String] = {
    document
      .get("urls")
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .map(url => url.split("//")(1).split("/")(0))
      .toList
      .map(mergeSameUrls)
  }

  def mergeSameUrls(str: String): String = {
    str match {
      case "youtu.be" => "www.youtube.com"
      case "fb.me" => "www.facebook.com"
      case _ => str
    }
  }
}
