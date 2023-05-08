import IOUtils.*
import Matrix.*
import NumericUtils.*
import java.io.File
import java.util.zip.GZIPInputStream
import scala.util.Using

val maxDistance = 0.7
val distanceMetric = "damerau"
val computeDistance = StringDistance(distanceMetric)

List("surnames", "male-names", "female-names").foreach { datasetName =>

  val datasetSize =
    Using(File(s"data/$datasetName.txt").toSource())(_.getLines().size).get

  val source =
    File(s"results/scores/$distanceMetric/${datasetName}_scores.txt.gz")
      .toInputStream().toGZIP().toSource()
      
  val scores = Using(source) {
      _.getLines()
        .map { line =>
          val Array(s1, s2, d) = line.split("\t")
          (s1, s2, d.toDouble)
        }
        .takeWhile(_._3 < maxDistance)
        .toSeq
  }
    .get

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

  val distances = matrix.distances
  val profileSizes =
    distances
      .map(matrix.profiles)
      .map(_.size)
      .map(size => (size, (size - 1) / (datasetSize - 1).toDouble))
  val distanceData =
    distances
      .zip(profileSizes)
      .map { p =>
        val (distance, (size, normalizedSize)) = p
        val metric = normalizedSize * (1.0 - distance)
        (distance, size, normalizedSize, metric)
      }

  val bestDistanceData = distanceData.maxBy(_._4)
  println(s"$datasetName($datasetSize): ${distances.size}, ${bestDistanceData._1}")

  File(s"results/scores/$distanceMetric/${datasetName}_profiles.txt")
    .writeLines(distanceData) { (distance, size, normalizedSize, metric) =>
      List(
        f"$distance%.08f",
        f"$size%06d",
        f"$normalizedSize%.08f",
        f"$metric%.08f",
      )
        .mkString("\t")
  }
}

extension [A](matrix: Map[A, Map[A, Double]])
  def profiles(distance: Double): Set[Set[A]] =
    matrix.values.toSeq
      .map(pairs => pairs.toSeq.filter(_._2 <= distance).map(_._1).toSet)
      .toSet
