import IOUtils.*
import StringDistance.*
import Utils.*
import info.debatty.java.stringsimilarity.Damerau

import java.io.*
import scala.io.Source
import scala.sys.process.*
import scala.util.Using

val distanceMetric = "damerau"

val inputDir = File("data")
val outputDir = File(s"results/scores/$distanceMetric")
outputDir.mkdirs()

List("surnames", "male-names", "female-names")
  .map(filename =>
    (
      File(inputDir, s"$filename.txt"),
      File(outputDir, s"${filename}_scores.txt.gz")
    )
  )
  .foreach { (inputFile, outputFile) =>
    print(s"Generating scores for ${inputFile.getName}... ")
    val (_, elapsedTime) = time(buildScoreFile(inputFile, outputFile))
    println(s"$elapsedTime milliseconds")
  }

def buildScoreFile(inputFile: File, outputFile: File) =
  Using(Source.fromFile(inputFile)) { in =>
    in.getLines()
      .map(_.split("\\s+")(0))
      .toIndexedSeq
      .distinct
      .sorted
  }
    .map { values =>
      // FIXME Hangs when placed outside block
      val stringDistance = StringDistance(distanceMetric)

      sortAndCompress(outputFile) { out =>
        LazyList
          .from(values.indices)
          .flatMap(i => (i + 1 until values.size).map(j => (i, j)))
          .map((i, j) =>
            (values(i), values(j), stringDistance(values(i), values(j)))
          )
          .filter(_._3 != 1.0)
          .map(_.toList.mkString("\t"))
          .foreach(out.println)
      }
    }

def sortAndCompress(outputFile: File)(writer: PrintWriter => Unit): Int =
  val commandLine =
    List(
      "sort -t'\t' -k3,3n -k1,1 -k2,2",
      s"gzip > '${outputFile.getAbsolutePath}'"
    )
      .mkString(" | ")
  val processIO =
    BasicIO
      .standard(true)
      .withInput(os => Using(os.toPrintWriter())(writer))
  Process(s"sh -c \"$commandLine\"").run(processIO).exitValue()
