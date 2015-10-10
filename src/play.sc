import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


class Edge(val fromId: Int, val toId: Int, val fromLabel: Int, val edgeLabel: Int, val toLabel: Int) {
  override def toString = s"Edge: ($fromId, $toId, $fromLabel, $edgeLabel, $toLabel)"

}

class Vertex(val id: Int, val label: Int, val edges: List[Edge]) {
  def this(id: Int, label: Int) = this(id, label, List())


  val tempEdges = new ListBuffer[Edge]()


  def addEdge(edge: Edge): Unit = {
    tempEdges += edge
  }
}

class EdgeCode(val fromId: Int, val toId: Int, val fromLabel: Int, val edgeLabel: Int, val toLabel: Int) {
  override def toString = s"Edgecode: ($fromId, $toId, $fromLabel, $edgeLabel, $toLabel)"

  def this() = this(-1, -1, -1, -1, -1)

  def toTupe = (fromId, toId, fromLabel, edgeLabel, toLabel)

}

class DFSCode(codes: List[EdgeCode], graphSet: List[(Int, Map[Int, Int])]) {
  def info = {
    val header = "fromId\ttoId\tfromLabel\tedgeLabel\ttoLabel\n"
    val body = codes.map(code => s"${code.fromId}\t\t${code.toId}\t\t${code.fromLabel}\t\t\t${code.edgeLabel}" +
      s"\t\t\t${code.toLabel}").mkString("\n")
    header + body
  }
}

class FinalDFSCode(codes: List[EdgeCode], support: Int) {
}

class Graph(val id: Int, val vertices: List[Vertex]) {

  // TODO: Should use Array to store vertices for faster retrieval?

  def this(id: Int) = this(id, List())

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

class GraphSet(val graphSet: List[Graph])

def loadDataFileAndCount(filename: String) = {
  val vertexLabelCounter = new mutable.HashMap[Int, Int]()
  val edgeLabelCounter = new mutable.HashMap[Int, Int]()

  val tempGraphList = new ListBuffer[Graph]()

  var currentGraph: Graph = null

  for(line <- Source.fromFile(filename).getLines){
    val segments = line.split(" ")
    val lineType = segments(0)
    lineType match {
      case "t" => {
        // New Graph
        val graphId = segments(2).toInt
        if (currentGraph != null) {
          tempGraphList += currentGraph
        }
        if (graphId != -1) {
          currentGraph = new Graph(graphId)
        }
      }
      case "v" => {

        val vertesId = segments(1).toInt
        val vertexLabel = segments(2).toInt
        vertexLabelCounter.put(vertexLabel, vertexLabelCounter.getOrElse(vertexLabel, 0) + 1)
        val vertex = new Vertex(vertesId, vertexLabel)
        currentGraph.addVertex(vertex)
      }
      case "e" => {

        val fromId    = segments(1).toInt
        val toId      = segments(2).toInt
        val edgeLabel = segments(3).toInt
        edgeLabelCounter.put(edgeLabel, edgeLabelCounter.getOrElse(edgeLabel, 0) + 1)

        val fromVertex = currentGraph.findVertexSlow(fromId)
        val toVertex = currentGraph.findVertexSlow(toId)

        val fromEdge = new Edge(fromId, toId, fromVertex.label, edgeLabel, toVertex.label)
        val toEdge = new Edge(toId, fromId, toVertex.label, edgeLabel, fromVertex.label)

        fromVertex.addEdge(fromEdge)
        toVertex.addEdge(toEdge)

      }
    }
  }
  (tempGraphList, vertexLabelCounter, edgeLabelCounter)
}


