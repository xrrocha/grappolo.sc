import Utils.*
import java.io.{File, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Using

List("surnames", "male-names", "female-names")
  .map(Experiment("01-agglomeration", "damerau", 0.4, _))
  .foreach { experiment =>
    import experiment.*

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
