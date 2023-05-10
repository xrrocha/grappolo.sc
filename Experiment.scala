import Utils.*
import java.io.{File, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Using

class Experiment(
    experimentName: String,
    distanceMetricName: String,
    maxDistance: Double,
    datasetName: String
):

  log("Experiment:", experimentName)
  log("Data set:", datasetName)
  log("Distance metric:", distanceMetricName)
  log("Max distance:", maxDistance)

  val computeDistance = StringDistance(distanceMetricName)
  val (entries, scoreIterator) =
    loadScores(dataDirectory, datasetName, distanceMetricName)

  val scores: Seq[(String, String, Double)] =
    scoreIterator
      .takeWhile((_, _, distance) => distance < maxDistance)
      .toSeq

  val distances = scores.map(_._3).distinct.sorted
  log(distances.size.asCount, "distances")

  private lazy val dataDirectory = File("data")
  def dataFile(basename: String, extension: String = "txt") =
    File(dataDirectory, s"$basename.$extension")

  private lazy val resultDirectory =
    val directory = File(
      File("results"),
      List(experimentName, distanceMetricName, datasetName, maxDistance)
        .mkString(File.separator)
    )
    directory.mkdirs()
    directory
  def resultFile(basename: String, extension: String = "txt") =
    File(resultDirectory, s"$basename.$extension")
  def saveResult[A](basename: String)(action: PrintWriter => A) =
    resultFile(basename).write(action)

  lazy val logWriter = resultFile("experiment", "log").toPrintWriter()
  // FIXME Support interspersed punctuation
  def log(msg: Any*) =
    val now = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
    val line = s"[$now] ${msg.mkString(" ")}"
    println(line)
    logWriter.println(line)

  def loadScores(
      dataDir: File,
      dataset: String,
      distanceMetricName: String
  ): (IndexedSeq[String], Iterator[(String, String, Double)]) =

    val inputFile = File(dataDir, s"$dataset.txt")
    val scoreFile = File(dataDir, s"$dataset-scores.txt.gz")

    val entries =
      Using(Source.fromFile(inputFile)) { in =>
        in.getLines()
          .map(_.split("\\s+")(0))
          .toIndexedSeq
          .distinct
          .sorted
      }.get

    if !(scoreFile.isFile() &&
        inputFile.lastModified() < scoreFile.lastModified())
    then populateScores(entries, scoreFile, distanceMetricName)

    // FIXME Source is left unclosed
    val scores =
      scoreFile
        .toInputStream()
        .toGZIP()
        .toSource()
        .mappingLines { line =>
          val Array(s1, s2, d) = line.split("\t")
          (s1, s2, d.toDouble)
        }

    (entries, scores)
  end loadScores

  private def populateScores(
      entries: Seq[String],
      outputFile: File,
      distanceMetricName: String
  ): Int =
    val stringDistance = StringDistance(distanceMetricName)
    val scores =
      LazyList
        .from(entries.indices)
        .flatMap(i => (i + 1 until entries.size).map(j => (i, j)))
        .map((i, j) =>
          (entries(i), entries(j), stringDistance(entries(i), entries(j)))
        )
        .filter(_._3 != 1.0)
    val commandLine =
      List(
        "sort -t'\t' -k3,3n -k1,1 -k2,2",
        s"gzip > '${outputFile.getAbsolutePath}'"
      )
        .mkString(" | ")
    val exitCode = OSCommand.writeLines(scores, commandLine) {
      _.toList.mkString("\t")
    }
    require(exitCode == 0)
    exitCode
  end populateScores
end Experiment
