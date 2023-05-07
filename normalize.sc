import java.io.*
import scala.io.Source
import scala.util.Using
import FileUtils.*
import NumberUtils.*

// val args = Array[String]()
Using(Source.fromFile(args(0))) { in =>
  val records =
    in.getLines()
      .map(_.split("\\s+").map(_.toDouble).toList)
      .toSeq
  Using(file2Writer(File(args(1)))) { out =>
    normalizeRecords(records)
      .zip(records)
      .map((n, r) => n ++ r)
      .map(record => record.map(v => f"$v%.08f"))
      .map(_.mkString("\t"))
      .foreach(out.println)
  }.get
}.get
