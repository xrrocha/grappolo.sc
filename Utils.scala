import info.debatty.java.stringsimilarity.{Damerau, NormalizedLevenshtein}
import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.io.{InputStream, OutputStream}
import java.io.{OutputStreamWriter, PrintWriter, Writer}
import java.text.DecimalFormat
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.math.Numeric.Implicits.infixNumericOps
import scala.math.max
import scala.util.{Try, Using}

object Utils:

  export IOUtils.*
  export NumericUtils.*
  export StringDistance.*

  implicit class KLike[T](t: T):
    def let[R](f: T => R): R = f(t)
    def also(f: T => Unit): T = { f(t); t }

  def time[A](action: => A): (A, Long) =
    val startTime = System.currentTimeMillis()
    (action, System.currentTimeMillis() - startTime)
end Utils

object IOUtils:
  extension (source: Source)
    def mapLines[A](action: String => A): Seq[A] =
      Using(source)(_.getLines().map(action)).get.toSeq
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

object NumericUtils:
  val CountFormat = DecimalFormat("###,###,###")
  val DistanceFormat = DecimalFormat("###,###,###.########")

  def normalize[N: Numeric](ns: Iterable[N]): Iterable[Double] =
    val min = ns.min.toDouble
    val denom = ns.max.toDouble - min
    ns.map(n => (n.toDouble - min) / denom)
  end normalize

  def normalizedMap[N: Numeric](ns: Iterable[N]): Map[N, Double] =
    ns.zip(normalize(ns)).toMap

  def normalizeRecords[N: Numeric](
      records: Iterable[Iterable[N]]
  ): List[List[Double]] =
    val fieldCount = records.head.size
    records
      .foldLeft(List.fill(fieldCount)(List[N]())): (accum, values) =>
        accum.zip(values).map((list, value) => list :+ value)
      .map(list => normalize(list))
      .foldLeft(List.fill(records.size)(List[Double]())): (accum, values) =>
        accum.zip(values).map((record, value) => record :+ value)
  end normalizeRecords

  extension [T: Numeric](num: T)
    def asCount = CountFormat.format(num)
    def asDistance = DistanceFormat.format(num)

  extension [T: Numeric](ns: Iterable[T])
    def avg: Double =
      if ns.isEmpty then 0.0
      else ns.sum.toDouble / ns.size
end NumericUtils

object OSCommand:
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
          )
        )
    Process(s"sh -c \"$commandLine\"").run(processIO).exitValue()
end OSCommand

object StringDistance:
  val computeDamerauDistance: (String, String) => Double =
    val damerau = Damerau()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else damerau.distance(e1, e2) / max(e1.length, e2.length).toDouble

  val computeLevenshteinDistance: (String, String) => Double =
    val levenshtein = NormalizedLevenshtein()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else levenshtein.distance(e1, e2)

  private val distanceMetrics = Map(
    "damerau" -> computeDamerauDistance,
    "levenshtein" -> computeLevenshteinDistance
  )
  def apply(metricName: String) = distanceMetrics(metricName)
end StringDistance
