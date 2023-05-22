package demetrio.util

import scala.util.Using

object OSUtils:
  def writeLines[A](items: Iterable[A], commandLine: String)(
      formatter: A => String
  ): Int =
    import IOUtils.*
    import scala.sys.process.{BasicIO, Process}
    val processIO =
      BasicIO
        .standard(true)
        .withInput(os =>
          Using(os.toPrintWriter())(out =>
            items.map(formatter).foreach(out.println)
          ).get
        )
    Process(s"sh -c \"$commandLine\"").run(processIO).exitValue()
end OSUtils
