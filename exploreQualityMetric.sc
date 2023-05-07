import java.io.*
import scala.io.Source
import scala.util.Using

import scala.math.Numeric.Implicits.infixNumericOps

val metrics =
  Source
    .fromFile("metrics.txt")
    .getLines()
    .map(_.split("\\s+").map(_.toDouble))
    .map { arr =>
      val Array(distance, clusterCount, medoidDistance) = arr
      (distance, clusterCount, medoidDistance)
    }
    .toSeq
val (distances, clusterCounts, medoidDistances) =
  metrics
    .foldLeft((List[Double](), List[Double](), List[Double]())) {
      (accum, nums) =>
        val (distances, clusterCounts, medoidDistances) = accum
        val (distance, clusterCount, medoidDistance) = nums
        (
          distances :+ distance,
          clusterCounts :+ clusterCount,
          medoidDistances :+ medoidDistance
        )
    }

val normalizedMetrics =
  def normalize[N: Numeric](ns: Iterable[N]): Iterable[Double] =
    val min = ns.min.toDouble
    val denom = ns.max.toDouble - min
    ns.map(n => (n.toDouble - min) / denom)
  end normalize
  normalize(distances)
    .zip(normalize(clusterCounts))
    .zip(normalize(medoidDistances))
    .map { t =>
      val ((distance, clusterCount), medoidDistance) = t
      (distance, clusterCount, medoidDistance)
    }
    .toSeq

Using(PrintWriter(FileWriter("normalized-metrics.txt"))) { out =>
  metrics
    .zip(normalizedMetrics)
    .map { (m, n) =>
      def quality(triplet: (Double, Double, Double)): Double =
        val (distance, clusterCount, medoidDistance) = triplet
        distance * clusterCount * medoidDistance

      val (distance, clusterCount, medoidDistance) = m
      val (
        normalizedDistance,
        normalizedClusterCount,
        normalizedMedoidDistance
      ) = n
      List(
        distance,
        clusterCount,
        medoidDistance,
        quality(m),
        normalizedDistance,
        normalizedClusterCount,
        normalizedMedoidDistance,
        quality(n)
      )
    }
    .sortBy(_(7))
    .foreach { list =>
      val record = list.map(v => f"$v%.08f").mkString("\t")
      println(record)
      out.println(record)
    }
}
