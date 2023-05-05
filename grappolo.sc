import info.debatty.java.stringsimilarity.Damerau
import java.io.{File, FileWriter, PrintWriter}
import java.text.DecimalFormat
import java.time.format.DateTimeFormatter
import java.time.Instant
import scala.annotation.tailrec
import scala.io.Source
import scala.math.{max, min}
import scala.util.Using

val experimentName = "02-distance-agglomeration"
val datasetName = "female-names"
val distanceMetricName = "Damerau-Levenshtein"
val maxDistance = 0.4

val resultDirectory =
  val directory = File(
    File("results"),
    List(experimentName, datasetName, distanceMetricName, maxDistance)
      .mkString(File.separator)
  )
  directory.mkdirs()
  directory
def dataFile(basename: String, extension: String = "txt") =
  File(s"data/$basename.$extension")
def resultFile(basename: String, extension: String = "txt") =
  File(resultDirectory, s"$basename.$extension")
def saveResult[A](basename: String)(action: PrintWriter => A) =
  saveTo(resultFile(basename))(action)

val logWriter = file2Writer(resultFile("experiment", "log"))
def log(msg: Any*) =
  val now = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
  val line = s"[$now] ${msg.mkString(" ")}"
  println(line)
  logWriter.println(line)

log("Experiment:", experimentName)
log("Data set:", datasetName)
log("Distance metric:", distanceMetricName)
log("Max distance:", maxDistance)

val (values: Seq[String], scores: Seq[(String, String, Double)]) = make(
  dataFile(datasetName),
  _.split("\t")(0),
  resultFile("scores"),
  s => {
    val Array(s1, s2, d) = s.split("\t")
    (s1, s2, d.toDouble)
  },
  _.toList.mkString("\t")
) { values =>
  values.indices
    .flatMap(i => (i until values.size).map(j => (values(i), values(j))))
    .map((s1, s2) => (s1, s2, computeDistance(s1, s2)))
    .filter((_, _, d) => d < maxDistance)
    .sortBy(_._3)
}
log(values.size.asCount, "values", scores.size.asCount, "scores")

val matrix: Map[String, Map[String, Double]] =
  scores
    .groupBy((s1, _, _) => s1)
    .map { (s1, ss) =>
      s1 -> ss
        .map((_, s2, dist) => (s2, dist))
        .toMap
        .withDefault(computeDistance(s1, _))
    }
    .withDefault(s1 => Map().withDefault(s2 => computeDistance(s1, s2)))

extension (matrix: Map[String, Map[String, Double]])
  def apply(s1: String, s2: String): Double =
    if s1 == s2 then 0.0
    else
      val (i1, i2) = if s1 < s2 then (s1, s2) else (s2, s1)
      matrix
        .get(i1)
        .flatMap(row => row.get(i2))
        .getOrElse(computeDistance(i1, i2))

  def computeMedoids(cluster: Set[String]): Set[String] =
    def computeIntraDistances(cluster: Set[String]): Seq[(String, Double)] =
      cluster.toSeq
        .map(element =>
          (
            element,
            cluster.map(matrix(element, _)).avg
          )
        )
    def computeBestMembers(cluster: Set[String]): Set[String] =
      val intraDistances = computeIntraDistances(cluster)
      val minDistance =
        intraDistances.map(_._2).filter(_ > 0.0).minOption.getOrElse(0.0)
      intraDistances.filter(_._2 == minDistance).map(_._1).toSet
    def go(cluster: Set[String], bestMembers: Set[String]): Set[String] =
      if cluster == bestMembers then cluster
      else go(bestMembers, computeBestMembers(bestMembers))
    go(cluster, computeBestMembers(cluster))
  end computeMedoids

  def computeAvgMedoidDistance(cluster: Set[String]): Double =
    val medoids = computeMedoids(cluster)
    cluster
      .flatMap(element => medoids.map(medoid => matrix(element, medoid)))
      .avg

  def compare(c1: Set[String], c2: Set[String]): Double =
    c1.flatMap(i => c2.map(j => matrix(i, j))).avg

  def sort(cluster: Set[String]): Seq[(String, Double)] =
    val medoids = computeMedoids(cluster)
    cluster.toSeq
      .map(entry => (entry, medoids.map(matrix(_, entry)).avg))
      .sortBy((entry, distance) => (distance, entry))

saveResult("distances") { out =>
  val distances = scores.map(_._3).distinct.sorted
  log(distances.size.asCount, "distinct distances found")
  distances.foreach(out.println)
}

saveResult("clusters") { out =>

  def print(distance: Double, clusters: Seq[Set[String]]) =
    val medoidDistances = clusters.map(matrix.computeAvgMedoidDistance)
    val avgMedoidDistance = medoidDistances.avg
    log(
      "Dumping",
      clusters.size.asCount,
      "clusters for distance",
      distance.asDistance,
      "Quality:",
      avgMedoidDistance.asDistance,
      "Measure:",
      (clusters.size * avgMedoidDistance).asDistance
    )
    clusters
      .zip(medoidDistances)
      .map { (cluster, quality) =>
        (
          quality,
          cluster.size,
          matrix.sort(cluster).map(_._1).mkString(",")
        )
      }
      .sortBy((quality, size, cluster) => (-size, quality, cluster))
      .map { (quality, size, cluster) =>
        List(distance, quality, size, cluster).mkString("\t")
      }
      .foreach(out.println)
    out.println("#")
  end print

  val clustersByDistance: Seq[(Double, Seq[Set[String]])] =
    scores
      .groupBy((_, _, distance) => distance)
      .toSeq
      .map { (distance, values) =>
        val clusters =
          values
            .groupBy((s1, _, _) => s1)
            .toSeq
            .map { (s1, ss) =>
              ss.map((_, s2, _) => s2).toSet + s1
            }
        distance -> clusters
      }
      .sortBy((distance, clusters) => (distance, -clusters.size))

  val clusters =
    val initialClusters = values.map(s => (s, Set(s))).toMap
    clustersByDistance.foldLeft(initialClusters) { (runningClusters, elem) =>
      val (distance, clusters) = elem
      print(distance, runningClusters.values.toSeq.distinct)

      clusters.foldLeft(runningClusters) { (runningClusters, cluster) =>

        @tailrec
        def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
          if clusters.size < 2 then clusters
          else
            val pairs: Seq[(Int, Int, Double)] =
              clusters.indices
                .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
                .map((i, j) => (i, j, matrix.compare(clusters(i), clusters(j))))
                .sortBy((i, j, d) => (d, clusters(i).size + clusters(j).size))
            val (i, j, dist) = pairs.head
            if dist > distance then clusters
            else
              merge(
                (clusters(i) ++ clusters(j)) +:
                  clusters.indices.filter(c => c != i && c != j).map(clusters)
              )
        end merge

        val nextClusters = merge(cluster.map(runningClusters).toSeq)
        runningClusters ++
          nextClusters.flatMap(cluster =>
            cluster.map(entry => (entry, cluster))
          )
      }
    }
}

lazy val distanceMetric = Damerau()
def computeDistance: (String, String) => Double =
  (e1: String, e2: String) =>
    if e1 == e2 then 0.0
    else distanceMetric.distance(e1, e2) / max(e1.length, e2.length).toDouble

def make[A, B](
    filenameA: String,
    parseA: String => A,
    filenameB: String,
    parseB: String => B,
    formatB: B => String
)(mapper: IndexedSeq[A] => Seq[B]): (Seq[A], Seq[B]) =
  make(File(filenameA), parseA, File(filenameB), parseB, formatB)(mapper)

def make[A, B](
    fileA: File,
    parseA: String => A,
    fileB: File,
    parseB: String => B,
    formatB: B => String
)(mapper: IndexedSeq[A] => Seq[B]): (Seq[A], Seq[B]) =
  val as = Source.fromFile(fileA).getLines().map(parseA).toIndexedSeq
  val bs =
    if fileB.lastModified() > fileA.lastModified then
      Source.fromFile(fileB).getLines().map(parseB).toSeq
    else
      val bs = mapper(as)
      saveTo(fileB)(out => bs.map(formatB).foreach(out.println))
      bs
  (as, bs)
end make

def saveTo[A](filename: String)(action: PrintWriter => A): A =
  saveTo(File(filename))(action)
def saveTo[A](file: File)(action: PrintWriter => A): A =
  Using(file2Writer(file))(action).get
def file2Writer(file: File): PrintWriter = PrintWriter(FileWriter(file), true)

lazy val countFormat = DecimalFormat("###,###,###")
lazy val distanceFormat = DecimalFormat("###,###,###.########")
extension (num: Number)
  def asCount = countFormat.format(num)
  def asDistance = distanceFormat.format(num)

def time[A](action: => A): (A, Long) =
  val startTime = System.currentTimeMillis()
  (action, System.currentTimeMillis() - startTime)

import scala.math.Numeric.Implicits.infixNumericOps
extension [T: Numeric](ns: Iterable[T])
  def avg: Double =
    if ns.isEmpty then 0.0
    else ns.sum.toDouble / ns.size
