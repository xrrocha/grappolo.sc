import java.io.{File, FileWriter, PrintWriter}
import scala.util.Using

object FileUtils:

  def saveTo[A](filename: String)(action: PrintWriter => A): A =
    saveTo(File(filename))(action)

  def saveTo[A](file: File)(action: PrintWriter => A): A =
    Using(file2Writer(file))(action).get

  def file2Writer(file: File): PrintWriter = PrintWriter(FileWriter(file), true)
