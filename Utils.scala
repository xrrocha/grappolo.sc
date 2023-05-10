object Utils:

  export IOUtils.*
  export NumericUtils.*
  export StringDistance.*

  implicit class KLike[T](t: T):
    def let[R](f: T => R): R = f(t)
    def also(f: T => Unit): T = { f(t); t }

  def time[A](action: => A): (A, Long) =
    val startTime = System.currentTimeMillis()
    (action, System.currentTimeMillis() - startTime)
