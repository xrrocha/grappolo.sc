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
