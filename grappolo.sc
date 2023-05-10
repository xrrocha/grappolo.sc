import IOUtils.*

import Make.*
import Matrix.*
import NumericUtils.*
import Score.cartesianPairs
import StringDistance.*

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.math.{max, min}
import scala.util.Using

val experimentName = "01-agglomeration"
val distanceMetricName = "damerau"
val maxDistance = 0.4

val computeDistance = StringDistance(distanceMetricName)

List("surnames", "male-names", "female-names").foreach { datasetName =>

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

  val entries: Seq[String] =
    dataFile(datasetName).readLines().map(_.split("\t")(0))

  val scores: Seq[(String, String, Double)] =
    Using(
      File(s"results/scores/$distanceMetricName/${datasetName}_scores.txt.gz")
        .toInputStream()
        .toGZIP()
        .toSource()
    )(
      _.mappingLines { line =>
        val Array(s1, s2, d) = line.split("\t")
        (s1, s2, d.toDouble)
      }
        .takeWhile((_, _, distance) => distance < maxDistance)
        .toSeq
    ).get

  val distances = scores.map(_._3).distinct.sorted
  log(distances.size, "distances")

  val matrix =
    def default(s1: String, s2: String) =
      if s1 == s2 then 0.0
      else computeDistance(s1, s2)
    // (scores ++ scores.map(t => (t._2, t._1, t._3)))
    scores
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

  def distanceProfiles(distance: Double): Set[Set[String]] =
    matrix.values.toSeq
      .map(pairs => pairs.toSeq.filter(_._2 <= distance).map(_._1).toSet)
      .toSet

  val bestDistance: Double =
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

  val scoredClusters: Seq[Seq[(String, Double)]] =
    scores
      .groupBy(_._1)
      .toSeq
      .map((s1, ss) => ((s1, 0.0) +: ss.map(s => (s._2, s._3))).sortBy(_._1))
  log(scoredClusters.size, "scored clusters: ")
  resultFile("scored-clusters").writeLines(scoredClusters) { scoredCluster =>
    scoredCluster
      .map((s, d) => s"$s/$d")
      .mkString("\t")
  }

  val initialClusters: Map[String, Set[String]] =
    entries.map(s => (s, Set(s))).toMap

  val clusters: Seq[Seq[String]] =
    scoredClusters
      .map(_.filter(_._2 <= bestDistance).map(_._1).toSet)
      .foldLeft(initialClusters) { (runningClusters, cluster) =>

        def compare(c1: Iterable[String], c2: Iterable[String]): Double =
          c1.flatMap(s1 => c2.map(s2 => computeDistance(s1, s2))).avg

        @tailrec
        def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
          if clusters.size < 2 then clusters
          else
            val pairs: Seq[(Int, Int, Double)] =
              clusters.indices
                .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
                .map((i, j) => (i, j, compare(clusters(i), clusters(j))))
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
