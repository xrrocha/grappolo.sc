import info.debatty.java.stringsimilarity.Damerau
import scala.math.max

object StringDistance:
  lazy val damerau = Damerau()
  def computeDamerauDistance: (String, String) => Double =
    (e1: String, e2: String) =>
      if e1 == e2 then 0.0
      else damerau.distance(e1, e2) / max(e1.length, e2.length).toDouble
