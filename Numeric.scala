import java.text.DecimalFormat
import java.time.format.DateTimeFormatter
import scala.math.Numeric.Implicits.infixNumericOps

object Numeric:
  val CountFormat = DecimalFormat("###,###,###")
  val DistanceFormat = DecimalFormat("###,###,###.########")

  def normalize[N: Numeric](ns: Iterable[N]): Iterable[Double] =
    val min = ns.min.toDouble
    val denom = ns.max.toDouble - min
    ns.map(n => (n.toDouble - min) / denom)
  end normalize

  def normalizedMap[N: Numeric](ns: Iterable[N]): Map[N, Double] =
    ns.zip(normalize(ns)).toMap

  extension [T: Numeric](num: T)
    def asCount = CountFormat.format(num)
    def asDistance = DistanceFormat.format(num)

  extension [T: Numeric](ns: Iterable[T])
    def avg: Double =
      if ns.isEmpty then 0.0
      else ns.sum.toDouble / ns.size
