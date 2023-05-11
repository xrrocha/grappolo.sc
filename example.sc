import Utils.*
import java.io.File

val clusterer = Grappolo.clustererFor[String](
  generatePairs = Grappolo.cartesianProduct,
  distanceMetric = StringDistance("damerau"),
  maxDistance = 0.4
)

val entries =
  File("data/surnames.txt")
    .readLines()
    .map(_.split("\t")(0))

clusterer(entries)
  .sortBy(_.size)
  .zipWithIndex
  .map: (cluster, index) =>
    List(index + 1, cluster.size, cluster.toSeq.sorted.mkString(","))
      .mkString("\t")
  .foreach(println)
