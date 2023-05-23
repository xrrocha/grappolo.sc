package demetrio.util

import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.io.{InputStream, OutputStream}
import java.io.{OutputStreamWriter, PrintWriter, Writer}
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.util.{Try, Using}

object IOUtils:
  extension (source: Source)
    def mapLines[A](action: String => A): Seq[A] =
      Using(source)(_.getLines().map(action).toSeq).get
    def mappingLines[A](action: String => A): Iterator[A] =
      source.getLines().map(action)
    def readLines(): IndexedSeq[String] =
      Using(source)(_.getLines().toIndexedSeq).get
  end extension

  extension (is: InputStream)
    def toSource(): Source =
      Source.fromInputStream(is)

    def toGZIP() =
      GZIPInputStream(is)

    def mapLines[A](action: String => A): Seq[A] =
      is.toSource().mapLines(action)

    def readAll[A](action: (InputStream) => A): A =
      Using(is)(action).get
  end extension

  extension (printWriter: PrintWriter)
    def write[A](action: PrintWriter => A): A =
      Using(printWriter)(action).get

    def writeLines[A](values: Iterable[A])(action: A => String): Unit =
      Using(printWriter)(out => values.map(action).foreach(out.println)).get
  end extension

  extension (os: OutputStream)
    def toPrintWriter(): PrintWriter =
      PrintWriter(OutputStreamWriter(os), true)
  end extension

  extension (filename: String)
    def splitFilename: (String, String) =
      val pos = filename.lastIndexOf('.')
      if pos < 0 then (filename, "")
      else (filename.substring(0, pos), filename.substring(pos + 1))
    def basename: String =
      filename.splitFilename._1
    def extension: String =
      filename.splitFilename._2
  end extension

  extension (file: File)
    def parentFile: File =
      val parent = file.getParentFile
      if parent != null then parent
      else File(".")

    def ensureParentDirs(): Boolean =
      parentFile.mkdirs()

    def toInputStream(): InputStream =
      FileInputStream(file)

    def toSource(): Source =
      Source.fromFile(file)

    def toOutputStream(): OutputStream =
      file.ensureParentDirs()
      FileOutputStream(file)

    def toWriter(): Writer =
      file.ensureParentDirs()
      FileWriter(file)

    def toPrintWriter(): PrintWriter =
      PrintWriter(file.toWriter(), true)
    end toPrintWriter

    def readLines(): Seq[String] =
      file.toSource().readLines()

    def mapLines[A](action: String => A): Seq[A] =
      file.toSource().mapLines(action)

    def readAll[A](action: (InputStream) => A): A =
      file.toInputStream().readAll(action)

    def write[A](action: PrintWriter => A): A =
      file.toPrintWriter().write(action)

    def writeLines[A](values: Iterable[A])(action: A => String): Unit =
      file.toPrintWriter().writeLines(values)(action)
  end extension
end IOUtils
