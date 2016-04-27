package enron

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkContext}

class QueriesTest extends UnitSpec {

  @transient val sparkContext = new SparkContext(
    new SparkConf()
        .setAppName("enron-analysis-test")
        .setMaster("local[*]")
        .set("spark.ui.showConsoleProgress", "false")
  )

  private def createEmail(content: String): Email = {
    Email(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  }

  test("Test most direct receive query") {
    val email1 = createEmail {
      "Message-ID: <32336379.1075847585331.JavaMail.evans@thyme>\n" +
          "Date: Thu, 10 May 2001 02:20:00 -0700 (PDT)\n" +
          "From: steven.kean@enron.com\n" +
          "To: maureen.mcvicker@enron.com\n" +
          "Subject: Some subject"
    }
    val email2 = createEmail {
      "Message-ID: <32336380.1075847585331.JavaMail.evans@thyme>\n" +
          "Date: Thu, 10 May 2001 02:20:00 -0700 (PDT)\n" +
          "From: alan.turing@enron.com\n" +
          "To: maureen.mcvicker@enron.com\n" +
          "Subject: good day"
    }
    val email3 = createEmail {
      "Message-ID: <32336381.1075847585331.JavaMail.evans@thyme>\n" +
          "Date: Thu, 10 May 2001 02:20:00 -0700 (PDT)\n" +
          "From: axl.rose@enron.com\n" +
          "To: saul.hudson@enron.com\n" +
          "Subject: whats up"
    }
    val emailRdd = sparkContext.parallelize(Array(email1, email2, email3))
    val (receiver, receivedEmails) = Queries.mostDirectReceive(emailRdd)

    val expectedReceivedEmails = Set(email1.messageId, email2.messageId)
    val actualReceivedEmails = Set(receivedEmails.toList.map(_.messageId): _*)
    assert(receiver == "maureen.mcvicker@enron.com")
    assert(actualReceivedEmails == expectedReceivedEmails)
  }
}
