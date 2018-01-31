package bounded.akka.persistence.eventmaterializers

import java.io.{PrintWriter, StringWriter}

trait ExceptionWriter {

  def logException(t: Throwable): String = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}
