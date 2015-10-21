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
    time {
      setParallelismGlobally(14)
      //val ln = readLine()
      val minSupport = 500
      val (gs, vertexLabelCounter, edgeLabelCounter) = loadDataFileAndCount("/Users/qmzheng/Code/gSpan-Scala/data/graph.data")
      val s = new ListBuffer[FinalDFSCode]

      var (graphSet, s1) = removeInfrequentVerticesAndEdges(gs, minSupport, vertexLabelCounter, edgeLabelCounter)

      for (edgeCode <- s1) {
        val dfsGraphSet = projectWithOneEdge(graphSet, edgeCode)
        val support = dfsGraphSet.map(_._1).distinct.size
        val dfsCode = new DFSCode(IndexedSeq(edgeCode), dfsGraphSet, support)
        subgraphMining(graphSet, s, dfsCode, minSupport)
        graphSet = shrink(graphSet, edgeCode)
      }
    }
  }
}

}
