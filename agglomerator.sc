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

    import experiment.{log, resultFile, saveResult}

    val matrix =
      def defaultDistance(entry1: String, entry2: String) =
        if entry1 == entry2 then 0.0
        else experiment.computeDistance(entry1, entry2)
      // TODO Evaluate best distance for just scores
      (experiment.scores ++
        experiment.scores.map: (entry1, entry2, distance) =>
          (entry2, entry1, distance))
        .groupBy((entry1, entry2, distance) => entry1)
        .map: (entry1, neighbors) =>
          entry1 -> neighbors
            .map((_, entry2, distance) => (entry2, distance))
            .toMap
            .withDefault(entry2 => defaultDistance(entry1, entry2))
        .withDefault: entry1 =>
          Map[String, Double]()
            .withDefault(entry2 => defaultDistance(entry1, entry2))

    def entryDistance(entry1: String, entry2: String) =
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
        .flatMap: entry1 =>
          cluster2.map(entry2 => entryDistance(entry1, entry2))
        .avg

    val (bestDistance: Double, qualityMetric: Double) =
      def neighborhoodProfiles(distanceThreshold: Double): Set[Set[String]] =
        matrix.values.toSeq
          .map: neighbors =>
            neighbors.toSeq
              .filter((neighbor, distance) => distance <= distanceThreshold)
              .map((neighbor, distance) => neighbor)
              .toSet // TODO sorted in neighborhoodProfile?
          .toSet // TODO distinct in neighborhoodProfile?
      end neighborhoodProfiles

      experiment.distances
        .zip {
          experiment.distances
            .map(neighborhoodProfiles)
            .map: profile =>
              val size = profile.size
              // Normalize profile size w/respect to entry count
              (size - 1) / (experiment.entries.size - 1).toDouble
        }
        .maxBy((distance, normalizedSize) => normalizedSize * (1.0 - distance))
    log("Best distance", bestDistance, "quality metric", qualityMetric)

    val groupedScores: Seq[Seq[(String, Double)]] =
      experiment.scores
        .groupBy((entry, neighbor, distance) => entry)
        .toSeq
        .map: (entry, neighbors) =>
          val entryDistancePairs =
            (entry, 0.0) +:
              neighbors
                .filter((_, neighbor, distance) => distance <= bestDistance)
                .map((_, neighbor, distance) => (neighbor, distance))
          entryDistancePairs.sortBy((neighbor, distance) => neighbor)
    log(groupedScores.size.asCount, "grouped scores")
    resultFile("grouped-scores").writeLines(groupedScores):
      entryDistancePairs =>
        entryDistancePairs
          .map((entry, distance) => s"$entry/$distance")
          .mkString("\t")

    @tailrec
    def merge(clusters: Seq[Set[String]]): Seq[Set[String]] =
      if clusters.size < 2 then clusters
      else
        val scoreIndices: Seq[(Int, Int, Double)] =
          clusters.indices
            .flatMap(i => ((i + 1) until clusters.size).map(j => (i, j)))
            .map((i, j) => (i, j, clusterDistance(clusters(i), clusters(j))))
            .sortBy((i, j, d) => (d, clusters(i).size + clusters(j).size))
        val (i, j, dist) = scoreIndices.head
        if dist > bestDistance then clusters
        else
          merge:
            (clusters(i) ++ clusters(j)) +:
              clusters.indices
                .filter(c => c != i && c != j)
                .map(clusters)
    end merge

    val clusters: Seq[Seq[String]] =

      val initialClusters: Map[String, Set[String]] =
        experiment.entries
          .map: entry =>
            (entry, Set(entry))
          .toMap

      val clusterMap: Map[String, Set[String]] =
        val groupedScoreClusters: Seq[Set[String]] =
          groupedScores.map: entryDistancePairs =>
            entryDistancePairs
              .map((entry, distance) => entry)
              .toSet

        groupedScoreClusters.foldLeft(initialClusters):
          (runningClusters, cluster) =>
            runningClusters ++
              merge(cluster.map(runningClusters).toSeq)
                .flatMap(cluster => cluster.map(entry => (entry, cluster)))
      end clusterMap

      clusterMap.values
        .map: clusters =>
          clusters.toSeq.sorted
        .toSeq
        .distinct
    end clusters
    log(
      clusters.size.asCount,
      "clusters found for",
      experiment.entries.size.asCount,
      "entries in",
      experiment.dataset
    )

    saveResult("clusters"): out =>
      out.println(f"size\tcount\t${bestDistance}%.08f")
      clusters
        .sortBy(cluster => -cluster.size)
        .map: cluster =>
          List(
            cluster.size,
            cluster.toSeq.sorted.mkString(",")
          )
            .mkString("\t")
        .foreach(out.println)