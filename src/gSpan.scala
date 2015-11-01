/**
 * Created by qmzheng on 10/15/15.
 */

package gSpan{

import scala.collection.mutable.ListBuffer

object gSpanMain {

  import gSpan.dataStructure._
  import gSpan.Algorithm._

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: "+(System.nanoTime-s)/1e9+"s")
    ret
  }

  def setParallelismGlobally(numThreads: Int): Unit = { // Set thread pool size
    val parPkgObj = scala.collection.parallel.`package`
    val defaultTaskSupportField = parPkgObj.getClass.getDeclaredFields.find{
      _.getName == "defaultTaskSupport"
    }.get

    defaultTaskSupportField.setAccessible(true)
    defaultTaskSupportField.set(
      parPkgObj,
      new scala.collection.parallel.ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(numThreads)
      )
    )
  }


  def main(args: Array[String]) {
    if (args.size != 3) {
      println("Usage: java -jar gSpan-Scala.jar [Filename] [Support ratio] [Number of threads]")
      println("Example: java -jar gSpan-Scala.jar graph.data 0.1 8")
      return 0
    }

    val filename = args(0)
    val support = args(1).toDouble
    val threads = args(2).toInt

    setParallelismGlobally(threads)


    val (gs, vertexLabelCounter, edgeLabelCounter) = loadDataFileAndCount(filename)
    val minSupport = (gs.size * support).toInt

    val s = new ListBuffer[FinalDFSCode]

    var (graphSet, s1, vertexLabelMapping, edgeLabelMapping) = removeInfrequentVerticesAndEdges(gs, minSupport, vertexLabelCounter, edgeLabelCounter)

    for (edgeCode <- s1) {
      val dfsGraphSet = projectWithOneEdge(graphSet, edgeCode)
      val support = dfsGraphSet.map(_._1).distinct.size
      val dfsCode = new DFSCode(IndexedSeq(edgeCode), dfsGraphSet, support)
      subgraphMining(graphSet, s, dfsCode, minSupport)
      graphSet = shrink(graphSet, edgeCode)
    }
    printResult(s, vertexLabelMapping, edgeLabelMapping, vertexLabelCounter.filter(_._2 >= minSupport))
  }

}

}
