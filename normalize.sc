import java.io.*
import scala.io.Source
import scala.util.Using
import IOUtils.*
import NumericUtils.*

// val args = Array[String]()
Using(Source.fromFile(args(0))) { in =>
  val records =
    in.getLines()
      .map(_.split("\\s+").map(_.toDouble).toList)
      .toSeq
  Using(File(args(1)).toPrintWriter()) { out =>
    normalizeRecords(records)
      .zip(records)
      .map((n, r) => n ++ r)
      .map(record => record.map(v => f"$v%.08f"))
      .map(_.mkString("\t"))
      .foreach(out.println)
  }.get
}.get
