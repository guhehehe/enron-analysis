package enron

import java.io.File

object Utils {

  /** Return a list of files in `dir` and its sub directories. */
  def listFiles(dir: File): List[File] = {
    if (dir.isFile) {
      List(dir)
    } else {
      dir.listFiles().flatMap(listFiles(_)).toList
    }
  }

  def emailFileFilter(file: File): Boolean = {
    val dir = file.getParentFile()
    val name = file.getName()
    dir.getName.forall(_.isDigit) && name.endsWith(".txt")
  }

  /** Return a list of Enron email files in `dir` and its sub directories. */
  def listEmails(dir: String): List[Email] = {
    listFiles(new File(dir)).filter(emailFileFilter).map(Email(_))
  }
}
