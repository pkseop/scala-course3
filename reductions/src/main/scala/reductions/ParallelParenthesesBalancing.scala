package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def loop(acc: Int, chars: Array[Char]): Boolean = {
      if (!chars.isEmpty) {
        val c = chars.head
        if (c == '(')
          loop(acc + 1, chars.tail)
        else if (c == ')') {
          if(acc > 0)
            loop(acc - 1, chars.tail)
          else
            false
        } else
          loop(acc, chars.tail)
      } else {
        if (acc == 0)
          true
        else
          false
      }
    }
    loop(0, chars)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) : (Int, Int) = {
      if (idx < until) {
        val c = chars(idx)
        if (c == '(')
          traverse(idx + 1, until, arg1+1, arg2)
        else if (c == ')') {
          if(arg1 > 0)
            traverse(idx + 1, until, arg1-1, arg2)
          else
            traverse(idx + 1, until, arg1, arg2+1)
        } else
          traverse(idx + 1, until, arg1, arg2)
      } else
        (arg1, arg2)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if(until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val (l, r) = parallel(reduce(from, mid), reduce(mid, until))
        val diff = l._1 - r._2
        if(diff >= 0)
          (diff + r._1, l._2)
        else
          (r._1, -diff + l._2)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
