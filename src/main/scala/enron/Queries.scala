package enron

import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat

object Queries {

  /**
   * Return a list of receivers of `email` and the date they received it.
   *
   * @param email the email to get receivers from
   * @return a list of 2 tuple in the form (receiver, day), indicates that the receiver received one
   *         email on day `day`
   */
  private def getReceiversOnDay(email: Email): Iterable[(String, String)] = {
    email.receivers.map { receiver =>
      val day = DateTimeFormat.forPattern("yyyy-MM-dd").print(email.date)
      (receiver, day)
    }
  }

  /**
   * Return a number of receivers on each day given a list of (receiver, day).
   *
   * @param receivers a list of receiver and the day on which the email is received
   * @return the day and the number of email received on that day
   */
  private def getReceivedEmailsPerDay(
      receivers: Iterable[(String, String)]
  ): List[(String, Int)] = {

    receivers.groupBy(_._2).mapValues(_.size).map(v => v).toList.sortBy(_._1)
  }

  /**
   * This query answers "how many emails did each person receive each day".
   *
   * This query first transform the input into a paired RDD of (receiver, day), then group them by
   * receiver to get all emails received per receiver, then the number of emails each person
   * received on each day is the size of each subgroup.
   *
   * @param email a [[RDD]] of [[Email]]
   * @return a list of receivers and the number of emails they received on each day
   */
  def emailEachPersonReceivedEachDay(
      email: RDD[Email]
  ): Iterable[(String, List[(String, Int)])] = {

    email.flatMap(getReceiversOnDay)
        .groupBy(_._1)
        .mapValues(getReceivedEmailsPerDay)
        .collect()
  }

  /**
   * This query answers "who sent the largest number of broadcast emails".
   *
   * This method first filters out direct emails, then group the results by the original sender,
   * then the largest group will be the answer.
   *
   * @param email a [[RDD]] of [[Email]]
   * @return the email address of the person who sent the largest number of broadcast emails, and a
   *         list of broadcast emails she/he sent
   */
  def mostBroadcastSend(email: RDD[Email]): (String, Iterable[Email]) = {
    email.filter(_.receivers.size > 1)
        .groupBy(_.from)
        .max()(Ordering.by[(String, Iterable[Email]), Int](_._2.size))
  }

  /**
   * This query answers "who received the largest number of direct emails".
   *
   * This method first filters out broadcast emails, then group the results by the receiver, then
   * the largest group will be the answer.
   *
   * @param email a [[RDD]] of [[Email]]
   * @return the email address of the person who received the largest number of direct emails, and a
   *         list of direct emails she/he received
   */
  def mostDirectReceive(email: RDD[Email]): (String, Iterable[Email]) = {
    email.filter(_.receivers.size == 1)
        .groupBy(_.receivers.toList(0))
        .max()(Ordering.by[(String, Iterable[Email]), Int](_._2.size))
  }

  /**
   * Return the original sender and the first responder of an email thread.
   *
   * The sender is defined as the first email of the chronologically sorted list of emails, and the
   * first responder is defined as the first email in that list that has a different origin than the
   * original sender.
   *
   * @param emails a list of emails of the same thread
   * @return the optional sender email and an optional first responding email
   */
  private def identifySenderAndFirstResponder(
      emails: Iterable[Email]
  ): (Option[Email], Option[Email]) = {

    val sorted = emails.toList.sortBy(_.date.getMillis())
    val sender = if (sorted.head.isReply) None else Option(sorted.head)
    val responder = sender.flatMap(sender => getFirstResponder(sender.from, sorted))
    (sender, responder)
  }

  /**
   * Return the first responder from the given chronologically sorted list of emails.
   *
   * @param sender original sender's email address
   * @param emails a list of chronologically sorted emails
   * @return an optional receiver
   */
  private def getFirstResponder(sender: String, emails: Iterable[Email]): Option[Email] = {
    val receivers = emails.dropWhile(receiver => !receiver.isReply || receiver.from == sender)
    if (receivers.nonEmpty) Option(receivers.head) else None
  }

  /**
   * This query answers "find the `num` emails with the fastest response times".
   *
   * @param email a [[RDD]] of [[Email]]
   * @return
   */
  def topNRespond(email: RDD[Email], num: Int) = {
    email.filter(email => email.subject.nonEmpty && !email.isForward)
        .groupBy(_.subject)
        .filter { case (_, emails) => emails.size > 1 }
        .flatMap {
          case (_, emails) => {
            val (sender, responder) = identifySenderAndFirstResponder(emails)
            for {
              sender <- sender
              responder <- responder
            } yield (responder.date.getMillis() - sender.date.getMillis(), sender, responder)
          }
        }.sortBy(_._1).take(num)
  }
}
