import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.math.{max, min}
import java.io.{File, FileWriter, PrintWriter}

import FileUtils.*
import Make.*
import Matrix.*
import Numeric.*
import StringDistance.*
import Util.*

val experimentName = "02-distance-agglomeration"
val datasetName = "surnames"
val distanceMetricName = "Damerau-Levenshtein"
val computeDistance = computeDamerauDistance
val maxDistance = 0.5

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

val (
  values: Seq[String],
  scores: Scores[String]
) = make(
  dataFile(datasetName),
  _.split("\t")(0),
  resultFile("scores"),
  s => {
    val Array(s1, s2, d) = s.split("\t")
    Score(s1, s2, d.toDouble)
  },
  _.delimited("\t"),
  new Scores(_, computeDistance)
) { values =>
  values.indices
    .flatMap(i => (i until values.size).map(j => (i, j)))
    .map { (i, j) =>
      val s1 = values(i)
      val s2 = values(j)
      (s1, s2, computeDistance(s1, s2))
    }
    .filter(_._3 < maxDistance)
    .map((s1, s2, d) => Score(s1, s2, d))
}
log(values.size.asCount, "values", scores.size.asCount, "scores")

saveResult("distances") { out =>
  val distances = scores.map(_.distance).toSet.toSeq.sorted
  log(distances.size.asCount, "distinct distances found")
  distances.foreach(out.println)
}

val matrix = scores.asMatrix

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
      .groupBy(_.distance)
      .toSeq
      .map { (distance, values) =>
        val clusters =
          values
            .groupBy(_.a1)
            .toSeq
            .map { (a1, ss) =>
              ss.map(_.a2).toSet + a1
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
