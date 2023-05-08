import NumericUtils.*

case class Score[A](a1: A, a2: A, distance: Double):
  def asDelimitedWith(delimiter: String = "\t"): String =
    List(a1, a2, distance).mkString(delimiter)

object Score:
  def cartesianPairs[A](values: Seq[A]): LazyList[(A, A)] =
    LazyList
      .from(values.indices)
      .flatMap(i => (i until values.size).map(j => (i, j)))
      .map((i, j) => (values(i), values(j)))

  def buildMatrix[A](
      scores: Iterable[Score[A]],
      computeDistance: (A, A) => Double = (_: A, _: A) => 1.0):
  Map[A, Map[A, Double]] =
    scores
      .groupBy(_.a1)
      .map { (a1, ss) =>
        a1 -> ss
          .map(s => (s.a2, s.distance))
          .toMap
          .withDefault(a2 => computeDistance(a1, a2))
      }
      .withDefault(a1 =>
        Map[A, Double]().withDefault(a2 => computeDistance(a1, a2))
      )
end Score

case class Scores[A](
    scores: Iterable[Score[A]],
    computeDistance: (A, A) => Double
) extends Iterable[Score[A]]:
  lazy val asMatrix: Map[A, Map[A, Double]] =
    scores
      .groupBy(_.a1)
      .map { (a1, ss) =>
        a1 -> ss
          .map(s => (s.a2, s.distance))
          .toMap
          .withDefault(a2 => computeDistance(a1, a2))
      }
      .withDefault(a1 =>
        Map[A, Double]().withDefault(a2 => computeDistance(a1, a2))
      )
  override def iterator: Iterator[Score[A]] = scores.iterator
end Scores
object Scores:
  def cartesianPairs[A](values: Seq[A]): LazyList[(A, A)] =
    LazyList
      .from(values.indices)
      .flatMap(i => (i until values.size).map(j => (i, j)))
      .map((i, j) => (values(i), values(j)))

  def apply[A](
      as: Iterable[A],
      maxDistance: Double,
      computeDistance: (A, A) => Double
  ): Scores[A] =
    val values = as.toIndexedSeq
    val size = as.size
    apply(
      as,
      maxDistance,
      cartesianPairs(values),
      computeDistance
    )

  def apply[A](
      as: Iterable[A],
      maxDistance: Double,
      pairs: Iterable[(A, A)],
      computeDistance: (A, A) => Double
  ): Scores[A] =
    val values = as.toIndexedSeq
    val scores =
      pairs
        .map((a1, a2) => (a1, a2, computeDistance(a1, a2)))
        .filter((_, _, d) => d < maxDistance)
        .map((a1, a2, d) => Score(a1, a2, d))
    new Scores(scores, computeDistance)
end Scores

object Matrix:
  extension (matrix: Map[String, Map[String, Double]])
    def apply(a1: String, a2: String): Double =
      if a1 == a2 then 0.0
      else
        val (i1, i2) = if a1 < a2 then (a1, a2) else (a2, a1)
        matrix(i1)(i2)

    def distances: Seq[Double] =
      matrix.values.flatMap(_.values).toSet.toSeq.sorted

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
  end extension
end Matrix
