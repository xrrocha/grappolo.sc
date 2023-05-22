package grappolo.distance

import scala.math.Numeric.Implicits.infixNumericOps

object VectorDistance:
  def cosineSimilarity[A](
      vector1: Map[A, Double],
      vector2: Map[A, Double]
  ): Double =
    val (dotProduct, sqSum1, sqSum2) =
      (vector1.keys ++ vector2.keys)
        .foldLeft((0.0, 0.0, 0.0)): (accum, a) =>
          val s1 = vector1.getOrElse(a, 0.0)
          val s2 = vector2.getOrElse(a, 0.0)
          val (dotProduct, sqSum1, sqSum2) = accum
          (
            dotProduct + s1 * s2,
            sqSum1 + s1 * s1,
            sqSum2 + s2 * s2
          )
    val magnitude1 = math.sqrt(sqSum1)
    val magnitude2 = math.sqrt(sqSum2)

    if magnitude1 == 0 && magnitude2 == 0 then 0.0
    else dotProduct / (magnitude1 * magnitude2)
  end cosineSimilarity
end VectorDistance
