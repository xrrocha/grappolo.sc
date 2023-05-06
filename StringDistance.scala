import StringDistance.{computeDamerauDistance, computeLevenshteinDistance}
import info.debatty.java.stringsimilarity.{Damerau, NormalizedLevenshtein}

import scala.math.max

object StringDistance:

  val computeDamerauDistance: (String, String) => Double =
    val damerau = Damerau()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else damerau.distance(e1, e2) / max(e1.length, e2.length).toDouble

  val computeLevenshteinDistance: (String, String) => Double =
    val levenshtein = NormalizedLevenshtein()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else levenshtein.distance(e1, e2)

  private val distanceMetrics = Map(
    "damerau" -> computeDamerauDistance,
    "levenshtein" -> computeLevenshteinDistance
  )
  def apply(metricName: String) = distanceMetrics(metricName)
