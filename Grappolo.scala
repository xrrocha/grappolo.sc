object Grappolo:
  def clustererFor[A](
      generatePairs: Iterable[A] => Iterable[(A, A)],
      distanceMetric: (A, A) => Double,
      maxDistance: Double
  ): Iterable[A] => Seq[Set[A]] =
    //
    entries =>

      val scores: Seq[(A, A, Double)] =
        generatePairs(entries)
          .filter((entry, neighbor) => entry != neighbor)
          .map: (entry, neighbor) =>
            (entry, neighbor, distanceMetric(entry, neighbor))
          .filter: (_, _, distance) =>
            distance > 0.0 && distance <= maxDistance
          .toSeq

      val matrix: Matrix[A] =
        Matrix(entries.size, scores): (entry, neighbor) =>
          if entry == neighbor then 0.0
          else distanceMetric(entry, neighbor)

      val clusterMap: Map[A, Set[A]] =

        val initialClusterMap: Map[A, Set[A]] =
          entries
            .map(entry => (entry, Set(entry)))
            .toMap

        val clusteringDistance = matrix.maxProfileDistance

        scores
          .filter((_, _, distance) => distance <= clusteringDistance)
          .sortBy((_, _, distance) => distance)
          .foldLeft(initialClusterMap): (runningClusterMap, score) =>
            val (entry, neighbor, _) = score

            runningClusterMap ++
              matrix
                .agglomerate(
                  runningClusterMap(entry),
                  runningClusterMap(neighbor)
                )
                .flatMap: cluster =>
                  cluster.map(entry => (entry, cluster))
      end clusterMap

      clusterMap.values.toSeq.distinct
  end clustererFor
end Grappolo

object Matrix:
  def apply[A](
      size: Int,
      scores: Iterable[(A, A, Double)]
  )(
      defaultMapping: (A, A) => Double
  ): Matrix[A] =
    val symmetricScores: Iterable[(A, A, Double)] =
      scores.map: (entry, neighbor, distance) =>
        (neighbor, entry, distance)
    val map =
      (scores ++ symmetricScores)
        .groupBy((entry, _, _) => entry)
        .map: (entry, scores) =>
          entry ->
            scores
              .map((_, neighbor, distance) => (neighbor, distance))
              .toMap
              .withDefault(neighbor => defaultMapping(entry, neighbor))
        .withDefault: entry =>
          Map().withDefault(neighbor => defaultMapping(entry, neighbor))
    require(size >= map.size)
    new Matrix(size, map)
  end apply

  def cartesianProduct[A](entries: Iterable[A]): Iterable[(A, A)] =
    val seq = entries.toIndexedSeq
    LazyList
      .from(seq.indices)
      .flatMap(i => (i + 1 until seq.size).map(j => (i, j)))
      .map((i, j) => (seq(i), seq(j)))
end Matrix

import scala.annotation.tailrec
class Matrix[A](size: Int, map: Map[A, Map[A, Double]]):
  def distances: Seq[Double] =
    map.values
      .flatMap: neighbors =>
        neighbors.map((neighbor, distance) => distance)
      .toSeq
      .distinct
      .sorted

  lazy val maxProfileDistance: Double =
    def neighborhoodProfiles(distanceThreshold: Double): Set[Set[A]] =
      map.values.toSeq
        .map: neighbors =>
          neighbors.toSeq
            .filter((neighbor, distance) => distance <= distanceThreshold)
            .map((neighbor, distance) => neighbor)
            .toSet
        .toSet
    end neighborhoodProfiles
    // Choose distance yielding highest metric for neighborhood profile count
    distances
      .zip {
        distances
          .map(neighborhoodProfiles)
          .map: neighborhoodProfile =>
            val profileCount = neighborhoodProfile.size
            // Normalize profile count w/respect to given size
            (profileCount - 1) / (size - 1).toDouble
      }
      .maxBy: (distance, profileCountScore) =>
        val similarity = 1.0 - distance
        similarity * profileCountScore
      ._1
  end maxProfileDistance

  def agglomerate(
      cluster1: Set[A],
      cluster2: Set[A]
  ): Seq[Set[A]] =
    @tailrec
    def go(clusters: Seq[Set[A]]): Seq[Set[A]] =
      if clusters.size < 2 then clusters
      else
        val (i, j, distance) =
          Matrix
            .cartesianProduct(clusters.indices)
            .map: (i, j) =>
              (i, j, clusterDistance(clusters(i), clusters(j)))
            .toSeq
            .minBy: (i, j, distance) =>
              (distance, clusters(i).size + clusters(j).size)
        if distance > maxProfileDistance then clusters
        else
          go:
            (clusters(i) ++ clusters(j)) +:
              clusters.indices
                .filter(c => c != i && c != j)
                .map(clusters)
    go(IndexedSeq(cluster1, cluster2))
  end agglomerate

  def clusterDistance(cluster1: Set[A], cluster2: Set[A]): Double =
    val scores =
      cluster1.flatMap: entry1 =>
        cluster2.map(entry2 => map(entry1)(entry2))
    scores.sum / scores.size
  end clusterDistance
end Matrix
