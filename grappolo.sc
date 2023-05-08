import IOUtils.*

import Make.*
import Matrix.*
import NumberUtils.*
import Scores.cartesianPairs
import StringDistance.*

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.math.{max, min}

val experimentName = "01-agglomeration"
val distanceMetricName = "damerau"
val maxDistance = 0.5

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

  // TODO Store scores only once per dataset
  val (values: Seq[String], scores: Scores[String]) =
    Make
      .fromInputFile(dataFile(datasetName))
      .inputRecordParsedWith(_.split("\t")(0))
      .toResultFile(resultFile("scores"))
      .resultRecordParsedWith { record =>
        val Array(s1, s2, distance) = record.split("\t")
        Score(s1, s2, distance.toDouble)
      }
      .resultRecordFormattedWith(_.asDelimitedWith("\t"))
      .inputValuesTransformedWith { values =>
        cartesianPairs(values)
          .map { (s1, s2) =>
            Score(s1, s2, computeDistance(s1, s2))
          }
          .filter(_.distance < maxDistance)
      }
      .resultValuesReducedWith(Scores(_, computeDistance))
  log(values.size.asCount, "values", scores.size.asCount, "scores")

  saveResult("distances") { out =>
    val distances = scores.map(_.distance).toSet.toSeq.sorted
    distances.foreach(out.println)
    log(distances.size.asCount, "distinct distances found")
  }

  val matrix = scores.asMatrix

  case class Step(distance: Double, clusters: Seq[Set[String]]):
    val medoidDistances = clusters.map(matrix.computeAvgMedoidDistance)
    val avgMedoidDistance = medoidDistances.avg

    def print(out: PrintWriter) =
      log(
        "Dumping",
        clusters.size.asCount,
        "clusters for distance",
        distance.asDistance,
        "Avg medoid distance:",
        avgMedoidDistance.asDistance,
        "Quality:",
        (distance * avgMedoidDistance * (1.0 - clusters.size / values.size)).asDistance
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
  end Step

  val (_, steps) = saveResult("clusters") { out =>
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

    val initialClusters = values.map(s => (s, Set(s))).toMap

    clustersByDistance.foldLeft(
      (
        initialClusters,
        List(Step(0.0, initialClusters.values.toSeq.distinct))
      )
    ) { (accum, elem) =>

      val (distance, clusters) = elem
      val (runningClusters, steps) = accum

      val nextClusters =
        clusters.foldLeft(runningClusters) { (runningClusters, cluster) =>

          @tailrec
          def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
            if clusters.size < 2 then clusters
            else
              val pairs: Seq[(Int, Int, Double)] =
                clusters.indices
                  .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
                  .map((i, j) =>
                    (i, j, matrix.compare(clusters(i), clusters(j)))
                  )
                  .sortBy((i, j, d) => (d, clusters(i).size + clusters(j).size))
              val (i, j, dist) = pairs.head
              if dist > distance then clusters
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
      val step = Step(distance, nextClusters.values.toSeq.distinct)
      step.print(out)

      (nextClusters, steps :+ step)
    }
  }

  val qualityData =
    steps.map(step =>
      List(
        step.distance,
        step.avgMedoidDistance,
        step.clusters.size.toDouble
      )
    )
  val normalizedQualityData =
    (normalizeRecords(qualityData))
      .zip(qualityData)
      .map(p => p._1 ++ p._2)
  saveResult("qualityData") { out =>
    normalizedQualityData
      .map(qd => qd.map(v => f"$v%.08f").mkString("\t"))
      .foreach(out.println)
  }

  val bestStep =
    steps
      .zip(normalizedQualityData)
      .maxBy { (_, qualityData) =>
        val distance :: avgMedoidDistance :: clusterCount :: _ = qualityData: @unchecked
        avgMedoidDistance * distance * clusterCount
      }
      ._1
  log(
    "Best clustering found at",
    bestStep.distance,
    "with",
    bestStep.clusters.size,
    "clusters"
  )
  saveResult("bestClusters") { out =>
    out.println(f"size\tcount\t${bestStep.distance}%.08f")
    bestStep.clusters
      .zip(bestStep.medoidDistances)
      .sortBy((cluster, avgMedoidDistance) => (-cluster.size, avgMedoidDistance))
      .map((cluster, avgMedoidDistance) =>
          List(
            cluster.size,
            avgMedoidDistance,
            cluster.toSeq.sorted.mkString(",")
          )
            .mkString("\t")
      )
      .foreach(out.println)
  }
}
