import Utils.*
import java.io.{File, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Using

List("surnames", "male-names", "female-names")
  .map: datasetName =>
    Experiment(
      name = "01-agglomeration",
      metric = "damerau",
      maxDistance = 0.4,
      dataset = datasetName
    )
  .foreach: experiment =>
    import experiment.*

    val matrix =
      def defaultDistance(entry1: String, entry2: String) =
        if entry1 == entry2 then 0.0
        else computeDistance(entry1, entry2)
      // TODO Evaluate best distance for just scores
      (scores ++ scores.map((entry1, entry2, distance) => (entry2, entry1, distance)))
        .groupBy((entry1, entry2, distance) => entry1)
        .map: (entry1, neighbors) =>
          entry1 -> neighbors
            .map((_, entry2, distance) => (entry2, distance))
            .toMap
            .withDefault(entry2 => defaultDistance(entry1, entry2))
        .withDefault(entry1 =>
          Map[String, Double]().withDefault(entry2 => defaultDistance(entry1, entry2))
        )

    def distance(entry1: String, entry2: String) =
      if entry1 == entry2 then 0.0
      else
        val (first, second) =
          if entry1 < entry2 then (entry1, entry2)
          else (entry2, entry1)
        matrix(first)(second)

    def clusterDistance(
        cluster1: Iterable[String],
        cluster2: Iterable[String]
    ): Double =
      cluster1
        .flatMap(entry1 => cluster2.map(entry2 => distance(entry1, entry2)))
        .avg

    val bestDistance: Double =
      def distanceProfiles(distanceThreshold: Double): Set[Set[String]] =
        matrix.values.toSeq
          .map: neighbors =>
            neighbors.toSeq
              .filter(_._2 <= distanceThreshold)
              .map(_._1).toSet
          .toSet
      end distanceProfiles

      distances
        .zip {
          distances
            .map(distanceProfiles)
            .map(_.size)
            .map(size => (size - 1) / (entries.size - 1).toDouble)
        }
        .maxBy((distance, normalizedSize) => normalizedSize * (1.0 - distance))
        ._1
    log("Best distance", bestDistance)

    val groupedScores: Seq[Seq[(String, Double)]] =
      scores
        .groupBy(_._1)
        .toSeq
        .map((entry1, ss) =>
          ((entry1, 0.0) +: ss
            .filter(_._3 <= bestDistance)
            .map(s => (s._2, s._3)))
            .sortBy(_._1)
        )
    log(groupedScores.size.asCount, "grouped scores")
    resultFile("grouped-scores").writeLines(groupedScores): scoredCluster =>
      scoredCluster
        .map((s, d) => s"$s/$d")
        .mkString("\t")

    @tailrec
    def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
      if clusters.size < 2 then clusters
      else
        val pairs: Seq[(Int, Int, Double)] =
          clusters.indices
            .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
            .map((i, j) => (i, j, clusterDistance(clusters(i), clusters(j))))
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

    val clusters: Seq[Seq[String]] =
      val initialClusters: Map[String, Set[String]] =
        entries.map(s => (s, Set(s))).toMap
      val clusterMap =
        groupedScores
          .map(_.map(_._1).toSet)
          .foldLeft(initialClusters): (runningClusters, cluster) =>
            runningClusters ++
              merge(cluster.map(runningClusters).toSeq)
                .flatMap(cluster => cluster.map(entry => (entry, cluster)))
      clusterMap.values
        .map(_.toSeq.sorted)
        .toSeq
        .distinct
    log(
      clusters.size.asCount,
      "clusters found for",
      entries.size.asCount,
      "entries in",
      dataset
    )

    saveResult("clusters"): out =>
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
