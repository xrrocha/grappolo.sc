import java.text.DecimalFormat
import java.time.format.DateTimeFormatter
import scala.math.Numeric.Implicits.infixNumericOps

object NumberUtils:
  val CountFormat = DecimalFormat("###,###,###")
  val DistanceFormat = DecimalFormat("###,###,###.########")

  def normalize[N: Numeric](ns: Iterable[N]): Iterable[Double] =
    val min = ns.min.toDouble
    val denom = ns.max.toDouble - min
    ns.map(n => (n.toDouble - min) / denom)
  end normalize

  def normalizedMap[N: Numeric](ns: Iterable[N]): Map[N, Double] =
    ns.zip(normalize(ns)).toMap

  def normalizeRecords[N: Numeric](
      records: Iterable[Iterable[N]]
  ): Iterable[Iterable[Double]] =
    val fieldCount = records.head.size
    records
      .foldLeft(List.fill(fieldCount)(List[N]())) { (accum, values) =>
        accum.zip(values).map((list, value) => list :+ value)
      }
      .map(list => NumberUtils.normalize(list))
      .foldLeft(List.fill(records.size)(List[Double]())) { (accum, values) =>
        accum.zip(values).map((record, value) => record :+ value)
      }
  end normalizeRecords

  extension [T: Numeric](num: T)
    def asCount = CountFormat.format(num)
    def asDistance = DistanceFormat.format(num)

  extension [T: Numeric](ns: Iterable[T])
    def avg: Double =
      if ns.isEmpty then 0.0
      else ns.sum.toDouble / ns.size
