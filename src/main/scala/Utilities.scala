import org.bson.Document

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object Utilities {
  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSX")

  def getAllWithRegex(text: String, pattern: Regex): List[String] = {
    if (text.startsWith("RT @")) {
      List()
    } else {
      pattern.findAllIn(text).toList
    }
  }

  def getNouns(text: String): List[String] = {
    getAllWithRegex(text, "[A-ZÄÖÜẞ][\\w\\xc0-\\xff]{4,}".r)
  }

  def isRetweet(tweet: Document): Boolean = {
    getText(tweet).startsWith("RT @") || getText(tweet).startsWith("RT@")
  }

  def transformTwitterDate(date: String): LocalDateTime = {
    val formatted = date.replace('T', ' ')
    LocalDateTime.parse(formatted, dtf).plusHours(2)
  }


  //Methoden für Zugriff auf BSON Tweet-Darstellung
  def getText(document: Document): String = document.get("data").asInstanceOf[Document].getString("text")

  def getParty(document: Document): String = document.get("matching_rules").asInstanceOf[util.ArrayList[Document]].asScala.head.getString("tag")

  def getTimestamp(document: Document): LocalDateTime = transformTwitterDate(document.get("data").asInstanceOf[Document].getString("created_at"))

  def getSource(document: Document): String = document.get("data").asInstanceOf[Document].getString("source")

  def getHashtags(document: Document): List[String] = {
    if (document.get("data").asInstanceOf[Document].get("entities") == null || document.get("data").asInstanceOf[Document].get("entities").asInstanceOf[Document].get("hashtags") == null)
      List()
    else
      document
        .get("data").asInstanceOf[Document]
        .get("entities").asInstanceOf[Document]
        .get("hashtags").asInstanceOf[util.ArrayList[Document]].asScala
        .map(_.getString("tag")).toList
  }

  def getTime(document:Document):(Int,Int,Int,Int) = {
    val timeStamp = getTimestamp(document);
    (timeStamp.getYear,timeStamp.getMonth.getValue,timeStamp.getDayOfMonth,timeStamp.getHour)
  }

}
