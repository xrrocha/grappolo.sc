import java.io.*
import scala.util.Using

object FileUtils:

  def saveTo[A](filename: String)(action: PrintWriter => A): A =
    saveTo(File(filename))(action)

  def saveTo[A](file: File)(action: PrintWriter => A): A =
    Using(file2Writer(file))(action).get

  def os2Writer(os: OutputStream): PrintWriter =
    PrintWriter(OutputStreamWriter(os), true)

  def file2Writer(file: File): PrintWriter =
    val parentFile =
      val parent = file.getParentFile
      if parent != null then parent
      else File(".")
    parentFile.mkdirs()
    PrintWriter(FileWriter(file), true)
