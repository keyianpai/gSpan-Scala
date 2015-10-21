/**
 * Created by qmzheng on 10/15/15.
 */


package gSpan.dataStructure {

import scala.collection.mutable.ListBuffer

class Edge(val fromId: Int, val toId: Int, val fromLabel: Int, val edgeLabel: Int, val toLabel: Int) {
    override def toString = s"Edge: ($fromId, $toId, $fromLabel, $edgeLabel, $toLabel)"

  }

class EdgeCode(val fromId: Int, val toId: Int, val fromLabel: Int, val edgeLabel: Int, val toLabel: Int) {
  override def toString = s"Edgecode: ($fromId, $toId, $fromLabel, $edgeLabel, $toLabel)"

  def this() = this(-1, -1, -1, -1, -1)

  def toTuple = (fromId, toId, fromLabel, edgeLabel, toLabel)

}

class DFSCode(val codes: IndexedSeq[EdgeCode], val graphSet: List[(Int, Map[Int, Int])]) {
  def info = {
    val header = "fromId\ttoId\tfromLabel\tedgeLabel\ttoLabel\n"
    val body = codes.map(code => s"${code.fromId}\t\t${code.toId}\t\t${code.fromLabel}\t\t\t${code.edgeLabel}" +
      s"\t\t\t${code.toLabel}").mkString("\n")
    header + body
  }
}

class FinalDFSCode(codes: IndexedSeq[EdgeCode], support: Int) {
}

class Vertex(val id: Int, val label: Int, var edges: List[Edge]) {
  override def toString = s"Vertex: (ID: $id, Label: $label)"

  def this(id: Int, label: Int) = this(id, label, List())


  val tempEdges = new ListBuffer[Edge]()


  def addEdge(edge: Edge): Unit = {
    tempEdges += edge
  }
}

class Graph(val id: Int, val vertices: IndexedSeq[Vertex]) {


  def this(id: Int) = this(id, IndexedSeq())

  val tempVertices = new ListBuffer[Vertex]()
  val innerLookup: Map[Int, Vertex] = vertices.map { vertex => (vertex.id, vertex)}.toMap


  def addVertex(vertex: Vertex) = {
    tempVertices += vertex
  }

  def findVertex(vertexId: Int): Vertex = {
    innerLookup(vertexId)
  }

  def findVertexSlow(vertexId: Int): Vertex = tempVertices.find(v => v.id == vertexId).get
}

class GraphSet(val graphSet: IndexedSeq[Graph])


}
