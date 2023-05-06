import scala.io.Source
import java.io.File

import FileUtils.*

object Make:
  def make[A, B, C](
      fileA: File,
      parseA: String => A,
      fileB: File,
      parseB: String => B,
      formatB: B => String,
      reducer: Seq[B] => C
  )(mapper: Seq[A] => Seq[B]): (Seq[A], C) =
    val as = Source.fromFile(fileA).getLines().map(parseA).toIndexedSeq
    val bs =
      if fileB.lastModified() > fileA.lastModified then
        Source.fromFile(fileB).getLines().map(parseB).toSeq
      else
        val bs = mapper(as)
        saveTo(fileB)(out => bs.map(formatB).foreach(out.println))
        bs
    (as, reducer(bs))
  end make

  def make[A, B, C](
      filenameA: String,
      parseA: String => A,
      filenameB: String,
      parseB: String => B,
      formatB: B => String,
      reducer: Seq[B] => C
  )(mapper: Seq[A] => Seq[B]): (Seq[A], C) =
    make(File(filenameA), parseA, File(filenameB), parseB, formatB, reducer)(
      mapper
    )

  def fromInputFile(inputFile: File) =
    InputFile(inputFile)
  def fromInputFile(filename: String) =
    InputFile(File(filename))
  case class InputFile(inputFile: File):
    def inputRecordParsedWith[A](inputParser: String => A) =
      InputRecordParsedWith(inputFile, inputParser)
  case class InputRecordParsedWith[A](
      inputFile: File,
      inputParser: String => A
  ):
    def toResultFile(resultFile: File): ToResultFile[A] =
      ToResultFile(inputFile, inputParser, resultFile)
    def toResultFile(filename: String): ToResultFile[A] =
      toResultFile(File(filename))
  case class ToResultFile[A](
      inputFile: File,
      inputParser: String => A,
      resultFile: File
  ):
    def resultRecordParsedWith[B](resultParser: String => B) =
      ResultRecordParsedWith(
        inputFile,
        inputParser,
        resultFile,
        resultParser
      )
  case class ResultRecordParsedWith[A, B](
      inputFile: File,
      inputParser: String => A,
      resultFile: File,
      resultParser: String => B
  ):
    def resultRecordFormattedWith(resultFormatter: B => String) =
      ResultRecordFormattedWith(
        inputFile,
        inputParser,
        resultFile,
        resultParser,
        resultFormatter
      )
  case class ResultRecordFormattedWith[A, B](
      inputFile: File,
      inputParser: String => A,
      resultFile: File,
      resultParser: String => B,
      resultFormatter: B => String
  ):
    def inputValuesTransformedWith(transform: Seq[A] => Seq[B]) =
      InputValuesTransformedWith(
        inputFile,
        inputParser,
        resultFile,
        resultParser,
        resultFormatter,
        transform
      )
  case class InputValuesTransformedWith[A, B](
      inputFile: File,
      inputParser: String => A,
      resultFile: File,
      resultParser: String => B,
      resultFormatter: B => String,
      transform: Seq[A] => Seq[B]
  ):
    def resultValuesReducedWith[C](reduce: Seq[B] => C) =
      make(
        inputFile,
        inputParser,
        resultFile,
        resultParser,
        resultFormatter,
        reduce
      )(transform)
end Make
