package grappolo.distance

import info.debatty.java.stringsimilarity.{Damerau, NormalizedLevenshtein}

object StringDistance:

  private val distanceMetrics = Map(
    "damerau" -> computeDamerauDistance,
    "levenshtein" -> computeLevenshteinDistance
  )

  def apply(metricName: String) = distanceMetrics(metricName)

  lazy val computeDamerauDistance: (String, String) => Double =
    val damerau = Damerau()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else damerau.distance(e1, e2) / math.max(e1.length, e2.length).toDouble

  lazy val computeLevenshteinDistance: (String, String) => Double =
    val levenshtein = NormalizedLevenshtein()
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else levenshtein.distance(e1, e2)

  def ngrams(string: String, size: Int, step: Int = 1): Seq[String] =
    string.sliding(size, step).toSeq.distinct.sorted
end StringDistance
