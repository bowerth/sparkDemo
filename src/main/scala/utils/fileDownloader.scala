package utils

import sys.process._
import java.net.URL
import java.io.File
import scala.language.postfixOps

object fileUtils {

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
  // fileDownloader("http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words", "stop-words-en.txt")

}
