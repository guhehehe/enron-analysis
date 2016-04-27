package enron

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    if (args.size != 1) {
      println(s"usage: prog enron_root")
      println("  enron_root: root dir of the enron data corpus")
      sys.exit(1)
    }

    @transient val sparkContext = new SparkContext(
      new SparkConf()
          .setAppName("enron-analysis")
          .set("spark.ui.showConsoleProgress", "false")
    )

    val emails = Utils.listEmails(args(0))
    val emailRdd = sparkContext.parallelize(emails).cache()

    val emailPerPersonPerDay = Queries.emailEachPersonReceivedEachDay(emailRdd)
    emailPerPersonPerDay.foreach {
      case (receiver, receivedByDay) => {
        println(s"Receiver: $receiver")
        receivedByDay.foreach {
          case (day, num) => println(s"  day: $day, number: $num")
        }
      }
    }

    val mostBroadcastSend = Queries.mostBroadcastSend(emailRdd)
    println {
      val person = mostBroadcastSend._1
      val number = mostBroadcastSend._2.size
      s"$person sent the largest number($number) of broadcast emails"
    }

    val mostDirectReceive = Queries.mostDirectReceive(emailRdd)
    println {
      val person = mostDirectReceive._1
      val number = mostDirectReceive._2.size
      s"$person received the largest number($number) of direct emails"
    }

    val topFiveRespond = Queries.topNRespond(emailRdd, 5)
    println("Top five quickest responds are:")
    topFiveRespond.foreach {
      case (responseTime, sender, responder) => {
        println(s"  sender: ${sender.from} in ${sender.messageId}")
        println(s"  responder: ${responder.from} in ${responder.messageId}")
        println(s"  response time: ${responseTime / 1000}s")
        println()
      }
    }
  }
}
