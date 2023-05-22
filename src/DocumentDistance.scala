package grappolo.distance

import math.log
import VectorDistance.cosineSimilarity

object DocumentDistance:
  def tfidfMetric[A](groups: Seq[Seq[A]]): (Seq[A], Seq[A]) => Double =
    val elementTfidfs: Map[A, Map[Seq[A], Double]] =
      val groupCountLog = log(groups.size.toDouble)
      groups.flatten
        .groupBy(identity)
        .map: (element, values) =>
          val idf = groupCountLog - log(values.size)
          val tfidf =
            groups
              .map(group => (group, group.count(_ == element)))
              .filter(_._2 > 0)
              .map: (group, count) =>
                val tf = count.toDouble / group.size
                group -> tf * idf
              .toMap
              .withDefaultValue(0.0)
          element -> tfidf
    val vectors: Map[Seq[A], Map[A, Double]] =
      groups
        .map: group =>
          group -> group.map(element => element -> elementTfidfs(element)(group)).toMap
        .toMap
    (group1: Seq[A], group2: Seq[A]) =>
      1.0 - cosineSimilarity(vectors(group1), vectors(group2))
end DocumentDistance
