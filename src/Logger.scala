package demetrio.util

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter

class Logger(out: PrintWriter):
  def apply(items: Any*) =
    val now = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
    // TODO Add type-based arg formatting in Logger
    println(s"[$now] ${items.mkString(" ")}")
    out.println(s"[$now] ${items.mkString(" ")}")

object Logger:
  def loggerFor(filename: String): Logger =
    loggerFor(File(filename))
  def loggerFor(file: File): Logger =
    val parentFile =
      val parent = file.getParentFile
      if parent != null then parent
      else File(".")
    parentFile.mkdirs()
    Logger(PrintWriter(FileWriter(file), true))
