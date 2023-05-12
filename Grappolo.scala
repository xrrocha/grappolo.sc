object Grappolo:
  def clustererFor[A](
      generatePairs: Iterable[A] => Iterable[(A, A)],
      distanceMetric: (A, A) => Double,
      maxDistance: Double
  ): Iterable[A] => Seq[Set[A]] =

    entries =>
      val scores: Seq[(A, A, Double)] =
        generatePairs(entries)
          .filter((entry, neighbor) => entry != neighbor)
          .map: (entry, neighbor) =>
            (entry, neighbor, distanceMetric(entry, neighbor))
          .filter: (_, _, distance) =>
            distance > 0.0 && distance <= maxDistance
          .toSeq

      val matrix =
        def defaultMapping(entry: A, neighbor: A) =
          if entry == neighbor then 0.0
          else distanceMetric(entry, neighbor)
        val symmetricScores: Iterable[(A, A, Double)] =
          scores.map: (entry, neighbor, distance) =>
            (neighbor, entry, distance)
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

      val bestDistance: Double =
        val distances: Seq[Double] =
          scores
            .map((_, _, distance) => distance)
            .distinct
            .sorted
        def neighborhoodProfiles(distanceThreshold: Double): Set[Set[A]] =
          matrix.values.toSeq
            .map: neighbors =>
              neighbors.toSeq
                .filter((_, distance) => distance <= distanceThreshold)
                .map((neighbor, _) => neighbor)
                .toSet
            .toSet
        distances
          .zip {
            distances
              .map(neighborhoodProfiles)
              .map: neighborhoodProfile =>
                val profileCount = neighborhoodProfile.size
                (profileCount - 1) / (entries.size - 1).toDouble
          }
          .maxBy: (distance, normalizedProfileCount) =>
            val similarity = 1.0 - distance
            similarity * normalizedProfileCount
          ._1

      def agglomerate(cluster1: Set[A], cluster2: Set[A]): Seq[Set[A]] =
        def clusterDistance(cluster1: Set[A], cluster2: Set[A]): Double =
          val scores =
            cluster1.flatMap: entry1 =>
              cluster2.map(entry2 => matrix(entry1)(entry2))
          scores.sum / scores.size
        def go(clusters: Seq[Set[A]]): Seq[Set[A]] =
          if clusters.size < 2 then clusters
          else
            val (i, j, distance) =
              cartesianProduct(clusters.indices)
                .map: (i, j) =>
                  (i, j, clusterDistance(clusters(i), clusters(j)))
                .minBy: (i, j, distance) =>
                  (distance, clusters(i).size + clusters(j).size)
            if distance > bestDistance then clusters
            else
              go:
                (clusters(i) ++ clusters(j)) +:
                  clusters.indices
                    .filter(c => c != i && c != j)
                    .map(clusters)
        go(IndexedSeq(cluster1, cluster2))

      val clusterMap: Map[A, Set[A]] =
        val initialClusterMap: Map[A, Set[A]] =
          entries
            .map(entry => (entry, Set(entry)))
            .toMap
        scores
          .filter((_, _, distance) => distance <= bestDistance)
          .sortBy((_, _, distance) => distance)
          .foldLeft(initialClusterMap): (runningClusterMap, score) =>
            val (entry, neighbor, _) = score
            runningClusterMap ++
              agglomerate(runningClusterMap(entry), runningClusterMap(neighbor))
                .flatMap: cluster =>
                  cluster.map(entry => (entry, cluster))

      clusterMap.values.toSeq.distinct
  end clustererFor

  def cartesianProduct[A](entries: Iterable[A]): Iterable[(A, A)] =
    val seq = entries.toIndexedSeq
    LazyList
      .from(seq.indices)
      .flatMap(i => (i + 1 until seq.size).map(j => (i, j)))
      .map((i, j) => (seq(i), seq(j)))
end Grappolo
