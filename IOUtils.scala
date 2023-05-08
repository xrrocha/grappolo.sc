import java.io.*
import scala.io.Source
import scala.util.{Try, Using}

object IOUtils:

  extension (os: OutputStream)
    def toPrintWriter(): PrintWriter =
      PrintWriter(OutputStreamWriter(os), true)
  end extension

  extension (file: File)
    def toInputStream(): InputStream =
      FileInputStream(file)

    def toSource(): Source =
      Source.fromFile(file)

    def toPrintWriter(): PrintWriter =
      val parentFile =
        val parent = file.getParentFile
        if parent != null then parent
        else File(".")
      parentFile.mkdirs()
      PrintWriter(FileWriter(file), true)

    def readLines[A](action: String => A): Iterator[A] =
      Using(file.toSource())(source => source.getLines().map(action)).get

    def readAll[A](action: (InputStream) => A): A =
      Using(file.toInputStream())(action).get

    def write[A](action: PrintWriter => A): A =
      Using(file.toPrintWriter())(action).get

    def writeLines[A](values: Iterable[A])(action: A => String): Unit =
      Using(file.toPrintWriter())(out => values.map(action).foreach(out.println)).get
  end extension
