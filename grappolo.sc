import Utils.*
import java.io.{File, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Using

val experimentName = "01-agglomeration"
val distanceMetricName = "damerau"
val maxDistance = 0.4

val computeDistance = StringDistance(distanceMetricName)

List("surnames", "male-names", "female-names").foreach { datasetName =>

  val dataDirectory = File("data")
  def dataFile(basename: String, extension: String = "txt") =
    File(dataDirectory, s"$basename.$extension")

  val resultDirectory =
    val directory = File(
      File("results"),
      List(experimentName, datasetName, distanceMetricName, maxDistance)
        .mkString(File.separator)
    )
    directory.mkdirs()
    directory
  def resultFile(basename: String, extension: String = "txt") =
    File(resultDirectory, s"$basename.$extension")
  def saveResult[A](basename: String)(action: PrintWriter => A) =
    resultFile(basename).write(action)

  val logWriter = resultFile("experiment", "log").toPrintWriter()
  // FIXME Support interspersed punctuation
  def log(msg: Any*) =
    val now = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
    val line = s"[$now] ${msg.mkString(" ")}"
    println(line)
    logWriter.println(line)

  log("Experiment:", experimentName)
  log("Data set:", datasetName)
  log("Distance metric:", distanceMetricName)
  log("Max distance:", maxDistance)

  val (entries, scoreIterator) =
    loadScores(dataDirectory, datasetName, distanceMetricName)

  val scores: Seq[(String, String, Double)] =
    scoreIterator
      .takeWhile((_, _, distance) => distance < maxDistance)
      .toSeq

  val distances = scores.map(_._3).distinct.sorted
  log(distances.size.asCount, "distances")

  val matrix =
    def default(s1: String, s2: String) =
      if s1 == s2 then 0.0
      else computeDistance(s1, s2)
    // TODO Evaluate best distance for just scores
    (scores ++ scores.map(t => (t._2, t._1, t._3)))
      .groupBy(_._1)
      .map { (s1, ss) =>
        s1 -> ss
          .map(s => (s._2, s._3))
          .toMap
          .withDefault(s2 => default(s1, s2))
      }
      .withDefault(s1 =>
        Map[String, Double]().withDefault(s2 => default(s1, s2))
      )
  def distance(s1: String, s2: String) =
    if s1 == s2 then 0.0
    else
      val (i1, i2) = if s1 < s2 then (s1, s2) else (s2, s1)
      matrix(i1)(i2)
  def clusterDistance(c1: Iterable[String], c2: Iterable[String]): Double =
    c1.flatMap(s1 => c2.map(s2 => distance(s1, s2))).avg

  val bestDistance: Double =
    def distanceProfiles(distance: Double): Set[Set[String]] =
      matrix.values.toSeq
        .map(pairs => pairs.toSeq.filter(_._2 <= distance).map(_._1).toSet)
        .toSet
    distances
      .zip {
        distances
          .map(distanceProfiles)
          .map(_.size)
          .map(size => (size - 1) / (entries.size - 1).toDouble)
      }
      .maxBy { (distance, normalizedSize) =>
        normalizedSize * (1.0 - distance)
      }
      ._1
  log("Best distance", bestDistance)

  val groupedScores: Seq[Seq[(String, Double)]] =
    scores
      .groupBy(_._1)
      .toSeq
      .map((s1, ss) =>
        ((s1, 0.0) +: ss.filter(_._3 <= bestDistance).map(s => (s._2, s._3)))
          .sortBy(_._1)
      )
  log(groupedScores.size.asCount, "grouped scores")
  resultFile("grouped-scores").writeLines(groupedScores) { scoredCluster =>
    scoredCluster
      .map((s, d) => s"$s/$d")
      .mkString("\t")
  }

  val initialClusters: Map[String, Set[String]] =
    entries.map(s => (s, Set(s))).toMap

  val clusters: Seq[Seq[String]] =
    groupedScores
      .map(_.filter(_._2 <= bestDistance).map(_._1).toSet)
      .foldLeft(initialClusters) { (runningClusters, cluster) =>

        @tailrec
        def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
          if clusters.size < 2 then clusters
          else
            val pairs: Seq[(Int, Int, Double)] =
              clusters.indices
                .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
                .map((i, j) =>
                  (i, j, clusterDistance(clusters(i), clusters(j)))
                )
                .sortBy((i, j, d) => (d, clusters(i).size + clusters(j).size))
            val (i, j, dist) = pairs.head
            if dist > bestDistance then clusters
            else
              merge(
                (clusters(i) ++ clusters(j)) +:
                  clusters.indices
                    .filter(c => c != i && c != j)
                    .map(clusters)
              )
        end merge

        val nextClusters = merge(cluster.map(runningClusters).toSeq)
        runningClusters ++
          nextClusters.flatMap(cluster =>
            cluster.map(entry => (entry, cluster))
          )
      }
      .values
      .map(_.toSeq.sorted)
      .toSeq
      .distinct
  log(clusters.size.asCount, "clusters found for", datasetName)

  saveResult("clusters") { out =>
    out.println(f"size\tcount\t${bestDistance}%.08f")
    clusters
      .sortBy(-_.size)
      .map(cluster =>
        List(
          cluster.size,
          cluster.toSeq.sorted.mkString(",")
        )
          .mkString("\t")
      )
      .foreach(out.println)
  }
}

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

def populateScores(
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
