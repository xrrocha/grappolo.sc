import demetrio.util.IOUtils.*
import demetrio.util.Logger.*
import demetrio.util.NumericUtils.*
import demetrio.util.OSUtils.*
import demetrio.util.Utils.*
import grappolo.Grappolo
import grappolo.Grappolo.*
import grappolo.Types.*
import grappolo.distance.DocumentDistance.*
import grappolo.distance.StringDistance
import grappolo.distance.StringDistance.*
import grappolo.distance.VectorDistance.*
import java.io.File
import scala.util.Using

log("Starting cluster...")

val records: Seq[(Id, Name, Count)] =
  loadSchoolNames(dataFile("encuestas"))
log(s"${records.size} records read")

// TODO Fix shameless stop-list/idf-threshold hack
val wordIdfThreshold = 4.65
val stopList = Set(
  "ESC", // 1.7518425817323926
  "COL", // 2.784602599032165
  "DE", // 3.1185204227883503
  "JAR", // 4.6419424590421405
  "LA", // 5.040805705363225
  "DEL", // 5.146798744613526
  "JARD", // 5.303605775254942
  "O", // 5.360104538277859
  "A", // 5.3634139731792265
  "JARDIN", // 5.455696206546076
  "ESCUELA" // 5.538761745057322
)

val namesById: Map[Id, Set[Name]] =
  records
    .map((id, name, _) => (id, name))
    .groupBy(_._1)
    .map: (id, values) =>
      id -> values.map(_._2).toSet

LazyList
  .from(namesById.toSeq.sortBy(_._2.size))
  .foreach((id, names) => clusterNames(id, names))

def clusterNames(runId: Id, names: Set[Name]): Seq[Set[Name]] =
  log(s"$runId: Computing word mappings for ${names.size} names")
  val wordMappings = computeWordMappings(names)

  log(s"$runId: Normalizing ${names.size} names")
  val normalizedNameMap: Map[Seq[Word], Set[Name]] =
    normalizeNames(names, wordMappings)

  val normalizedNames = normalizedNameMap.keys.toSeq
  val pairs =
    pairsFor[Seq[Word], Word](normalizedNames, identity)

  val tfidf: (Seq[String], Seq[String]) => Distance =
    val distance = tfidfMetric(normalizedNames)
    (nn1, nn2) => f"${distance(nn1, nn2)}%.03f".toDouble

  Grappolo(normalizedNames, _ => pairs, tfidf, 0.4)
    .let(_._2)
    .flatMap: clusters =>
      clusters.map(normalizedNameMap)
    .also: clusters =>
      log(s"$runId: ${clusters.size} clusters found")
      resultFile(s"clusters/$runId").write: out =>
        clusters
          .sortBy(-_.size)
          .zipWithIndex
          .foreach: (cluster, index) =>
            cluster.foreach: name =>
              List(index, name)
                .mkString("\t")
                .also(out.println)
end clusterNames

log("Done clustering!")

def computeWordMappings(names: Set[Name]): Map[Word, Word] =
  val wordCounts: Map[Word, Count] =
    names
      .flatMap(tokenize)
      .groupBy(identity)
      .map: (word, occurrences) =>
        word -> occurrences.size

  val words = wordCounts.keys.toSeq.sorted

  val wordPairs: Set[(String, String)] =
    pairsFor(words: Seq[String], ngrams(_, 3, 1))

  val (wordDistance: Distance, wordClusters: Seq[Set[Word]]) =
    Grappolo[String](
      words,
      // TODO Refactor Grappolo to accept score filtering
      _ => wordPairs,
      StringDistance("damerau"),
      0.4
    )
  log(s"${wordClusters.size} word clusters at $wordDistance")

  wordClusters
    .flatMap: cluster =>
      val exemplar = cluster.maxBy(wordCounts)
      cluster.map(word => (word, exemplar))
    .toMap
end computeWordMappings

def pairsFor[A, B](as: Seq[A], extract: A => Iterable[B]): Set[(A, A)] =
  val buildPairs: A => Set[(B, B)] = pairPermutations[A, B](extract)
  val asWithPairs: Seq[(A, Set[(B, B)])] = as.map(a => (a, buildPairs(a)))
  val b2As: Map[B, Map[B, Set[A]]] =
    asWithPairs
      .flatMap((a, bs) => bs.map((b1, b2) => (b1, b2, a)))
      .groupBy(_._1)
      .map: (b1, triplets) =>
        b1 -> triplets
          .groupBy(_._2)
          .map: (b2, triplets) =>
            b2 -> triplets.map(_._3).toSet
  LazyList
    .from(asWithPairs.flatMap(_._2).distinct)
    .map((b1, b2) => b2As(b1)(b2))
    .map(cartesianProduct)
    .foldLeft(Set[(A, A)]()): (allPairs, pairs) =>
      allPairs ++ pairs
end pairsFor

def normalizeNames(
    names: Set[Name],
    wordMappings: Map[Word, Word]
): Map[Seq[Word], Set[Name]] =
  names
    .map: name =>
      val normalizedName =
        tokenize(name)
          .map(wordMappings)
          .filterNot(stopList.contains)
      normalizedName -> name
    .groupBy(_._1)
    .map: (name, values) =>
      name -> values.map(_._2).toSet

def tokenize(string: String): List[Word] =
  string
    .split("[^\\p{Alnum}]+")
    .filterNot(_.isEmpty)
    .toList

def loadSchoolNames(sourceFile: File): Seq[(Id, Name, Count)] =
  val destinationFile = resultFile(sourceFile)
  if destinationFile.isFile() then
    destinationFile
      .readLines()
      .map: line =>
        val Array(id, name, count) = line.split("\t")
        (id, name, count.toInt)
  else
    require(sourceFile.exists())
    Using(sourceFile.toSource()): source =>
      source
        .getLines()
        .drop(1)
        .map(_.split("\t").map(_.replace("'", "")).map(_.trim).toList)
        .filter(_(4).nonEmpty)
        .map: record =>
          val loc1 :: loc2 :: loc3 :: _ :: name :: _ = record: @unchecked
          val location =
            Seq(loc1, loc2, loc3)
              .map(i => f"${i.toInt}%02d")
              .mkString
          (location, name.toUpperCase)
        .toSeq
        .groupBy(identity)
        .toSeq
        .map: (locationName, values) =>
          val (location, name) = locationName
          val count = values.size
          (location, name, count)
    .get
      .also: records =>
        destinationFile.writeLines(records)(_.toList.mkString("\t"))
end loadSchoolNames

lazy val dataDir = File("data")
  .also(dir => require(dir.exists()))
def dataFile(basename: String, extension: String = "txt") =
  File(dataDir, s"$basename.$extension")
lazy val resultDir = File("results")
  .also(_.mkdirs())
def resultFile(basename: String, extension: String = "txt") =
  File(resultDir, s"$basename.$extension")
    .also(_.ensureParentDirs())
def resultFile(sourceFile: File) =
  File(resultDir, sourceFile.getName)

lazy val log = loggerFor(resultFile(scriptPath.basename, "log"))
