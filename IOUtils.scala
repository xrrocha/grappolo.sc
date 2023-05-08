import java.io.*
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.util.{Try, Using}

object IOUtils:

  extension (source: Source)
    def mapLines[A](action: String => A): Seq[A] =
     Using(source)(_.getLines().map(action)).get.toSeq
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
      Using(printWriter)(out =>
        values.map(action).foreach(out.println)
      ).get
  end extension

  extension (os: OutputStream)
    def toPrintWriter(): PrintWriter =
      PrintWriter(OutputStreamWriter(os), true)
  end extension

  extension (file: File)
    def ensureParentDirs(): Boolean =
      val parentFile =
        val parent = file.getParentFile
        if parent != null then parent
        else File(".")
      parentFile.mkdirs()
    end ensureParentDirs

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

    def mapLines[A](action: String => A): Seq[A] =
      file.toSource().mapLines(action)

    def readAll[A](action: (InputStream) => A): A =
      file.toInputStream().readAll(action)

    def write[A](action: PrintWriter => A): A =
      file.toPrintWriter().write(action)

    def writeLines[A](values: Iterable[A])(action: A => String): Unit =
      file.toPrintWriter().writeLines(values)(action)
  end extension
