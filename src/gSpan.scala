/**
 * Created by qmzheng on 10/15/15.
 */
package gSpan{

import scala.collection.mutable.ListBuffer

object gSpanMain {

  import gSpan.dataStructure._
  import gSpan.Algorithm._

  def main(args: Array[String]) {
    val minSupport = 500
    val (gs, vertexLabelCounter, edgeLabelCounter) = loadDataFileAndCount("/Users/qmzheng/Code/gSpan-Scala/data/graph.data")
    val s = new ListBuffer[FinalDFSCode]

    var (graphSet, s1) = removeInfrequentVerticesAndEdges(gs, minSupport, vertexLabelCounter, edgeLabelCounter)

    for (edgeCode <- s1){
      val dfsGraphSet = projectWithOneEdge(graphSet, edgeCode)
      val dfsCode = new DFSCode(List(edgeCode), dfsGraphSet)
      subgraphMining(graphSet, s, dfsCode, minSupport)
      graphSet = shrink(graphSet, edgeCode)
    }
  }
}

}
