package enron

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties
import javax.mail.Session
import javax.mail.internet.MimeMessage

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// A workaround when subclassing non-serializable class that doesn't have no-arg constructor, see
// http://stackoverflow.com/questions/12125076/java-deserialization-invalidclassexception-no-valid-constructor
class SerializableMimeMessage extends MimeMessage(Session.getDefaultInstance(new Properties()))

object Email {
  def apply(file: File) = new Email(file)

  def apply(is: InputStream) = new Email(is)
}

class Email(@transient private val inputStream: InputStream)
    extends SerializableMimeMessage with Serializable {
  parse(inputStream)

  def this(file: File) = this(new FileInputStream(file))

  val messageId = getMessageID
  var isReply = false
  var isForward = false
  val from: String = getAndSplitHeader("from").toList(0)
  val to: Set[String] = getAndSplitHeader("to")
  val cc: Set[String] = getAndSplitHeader("cc")
  val bcc: Set[String] = getAndSplitHeader("bcc")
  val receivers: Set[String] = to ++ cc ++ bcc

  val subject: String = {
    val subject = getHeader("subject", "").replaceAll("\n|\r|\t", " ").replaceAll("^ +| +$", "")
    if (checkIsReply(subject)) {
      isReply = true
      stripReply(subject)
    } else if (checkIsForward(subject)) {
      isForward = true
      stripForward(subject)
    } else subject
  }

  val date: DateTime = {
    val formatter = DateTimeFormat.forPattern("E, dd MMMMM yyyy HH:mm:ss Z (z)")
    formatter.parseDateTime(getHeader("date", ""))
  }

  private def splitField(field: String, delimiter: String = ","): Array[String] = {
    Option(field) match {
      case Some(field) => field.split(delimiter).map(_.replaceAll("\n|\r|\t|^ +| +$", ""))
      case _ => Array()
    }
  }

  private def getAndSplitHeader(field: String): Set[String] = {
    Set(splitField(getHeader(field, "")): _*)
  }

  private def checkIsReply(subject: String): Boolean = {
    subject.startsWith("RE: ") || subject.startsWith("Re: ")
  }

  private def checkIsForward(subject: String): Boolean = {
    subject.startsWith("FW: ") || subject.startsWith("Fw: ")
  }

  private def stripReply(subject: String): String = subject.replaceAll("^(RE: |Re: )", "")

  private def stripForward(subject: String): String = subject.replaceAll("^(FW: |Fw: )", "")
}
