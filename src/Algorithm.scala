
/**
 * Created by qmzheng on 10/15/15.
 */


import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import util.control.Breaks._
import scala.collection.parallel._


package gSpan {

import gSpan.dataStructure._

object Algorithm {

  /**
   * Read graph data from given filename
   * @param filename Input filename
   * @return (temp graph list, vertex label counter, edge label counter)
   */
  def loadDataFileAndCount(filename: String) = {
    val vertexLabelCounter = new mutable.HashMap[Int, mutable.Set[Int]]()
    val edgeLabelCounter = new mutable.HashMap[Int, mutable.Set[Int]]()

    val tempGraphList = new ListBuffer[Graph]()

    var currentGraph: Graph = null
    var graphId = -1

    for (line <- Source.fromFile(filename).getLines) {
      val segments = line.split(" ")
      val lineType = segments(0)
      lineType match {
        case "t" => {
          // New Graph
          graphId = segments(2).toInt
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
          if (!vertexLabelCounter.contains(vertexLabel)) {
            vertexLabelCounter.put(vertexLabel, mutable.Set())
          }
          vertexLabelCounter.get(vertexLabel).get += graphId
          val vertex = new Vertex(vertesId, vertexLabel)
          currentGraph.addVertex(vertex)
        }
        case "e" => {

          val fromId = segments(1).toInt
          val toId = segments(2).toInt
          val edgeLabel = segments(3).toInt

          if (!edgeLabelCounter.contains(edgeLabel)) {
            edgeLabelCounter.put(edgeLabel, mutable.Set())
          }
          edgeLabelCounter.get(edgeLabel).get += graphId

          val fromVertex = currentGraph.findVertexSlow(fromId)
          val toVertex = currentGraph.findVertexSlow(toId)

          val fromEdge = new Edge(fromId, toId, fromVertex.label, edgeLabel, toVertex.label)
          val toEdge = new Edge(toId, fromId, toVertex.label, edgeLabel, fromVertex.label)

          fromVertex.addEdge(fromEdge)
          toVertex.addEdge(toEdge)

        }
      }
    }
    (tempGraphList, vertexLabelCounter.map{case (k,v) => (k, v.size)}.toMap, edgeLabelCounter.map{case (k,v) => (k, v.size)}.toMap)
  }

  /**
   * Remove vertices and edges with infrequent labels, reconstruct the graph set and corresponding label mapping
   * @param tempGraphList
   * @param minSupport
   * @param vertexLabelCounter
   * @param edgeLabelCounter
   * @return (New graph set, frequent one-edges, vertex label mapping, edge label mapping)
   */
  def removeInfrequentVerticesAndEdges(tempGraphList: ListBuffer[Graph], minSupport: Int, vertexLabelCounter:Map[Int, Int], edgeLabelCounter: Map[Int, Int]) = {
    val vertexLabelMapping = vertexLabelCounter.filter(_._2 > minSupport).toSeq.sortBy(- _._2).zipWithIndex.map(pair => (pair._1._1, pair._2)).toMap
    val edgeLabelMapping = edgeLabelCounter.filter(_._2 > minSupport).toSeq.sortBy(- _._2).zipWithIndex.map(pair => (pair._1._1, pair._2)).toMap
    val (transformedGraphSet, oneEdgeCounter) = reconstructGraphSet(tempGraphList, vertexLabelMapping, edgeLabelMapping)
    val S1 = oneEdgeCounter.filter(_._2 >= 2 * minSupport).keys.toSeq.sorted.map(pair => new EdgeCode(0, 1, pair._1, pair._2, pair._3 ))
    (transformedGraphSet, S1, vertexLabelMapping, edgeLabelMapping)
  }

  /**
   * Give dfs code, build the right most path
   * @param dfsCode The dfs code
   * @return the right most path
   */
  def buildRightMostPath(dfsCode: DFSCode) = {
    val rightMostNode = dfsCode.codes.last
    var rightMostId = if (rightMostNode.fromId < rightMostNode.toId) rightMostNode.toId else rightMostNode.fromId
    val path = new ListBuffer[Int]
    path += rightMostId
    for (code <- dfsCode.codes.reverse) {
      if (code.toId == rightMostId && code.fromId < code.toId) {
        rightMostId = code.fromId
        path += code.fromId
      }
    }
    path.reverse.toArray
  }

  def reconstructGraphSet(tempGraphList: ListBuffer[Graph], vertexLabelMapping:Map[Int, Int], edgeLabelMapping:Map[Int, Int]) = {
    def _buildOneEdgeTuple(edge: Edge) = {
      val fromLabel = edge.fromLabel
      val toLabel = edge.toLabel
      val edgeLabel = edge.edgeLabel
      (math.min(fromLabel, toLabel), edgeLabel, math.max(fromLabel, toLabel))
    }
    val frequentVertexLabels = vertexLabelMapping.keys.toSet
    val frequentEdgeLabels = edgeLabelMapping.keys.toSet
    val oneEdgeCounter = new mutable.HashMap[(Int, Int, Int), Int]()
    val graphListBuffer = new ListBuffer[Graph]

    for(graph <- tempGraphList) {
      val frequentVertices = graph.tempVertices
                                  .filter(vertex => frequentVertexLabels.contains(vertex.label))
                                  .map(vertex => {

        val edges = vertex.tempEdges.filter(edge => (frequentEdgeLabels.contains(edge.edgeLabel) && frequentVertexLabels.contains(edge.fromLabel) && frequentVertexLabels.contains(edge.toLabel)))
                                    .map(edge => new Edge(edge.fromId, edge.toId, vertexLabelMapping.get(edge.fromLabel).get, edgeLabelMapping.get(edge.edgeLabel).get, vertexLabelMapping.get(edge.toLabel).get ))
                                    .toList
        edges.foreach(edge =>{
          val tuple = _buildOneEdgeTuple(edge)
          oneEdgeCounter.put(tuple, oneEdgeCounter.getOrElse(tuple, 0) + 1)
        } )
        new Vertex(vertex.id, vertexLabelMapping.get(vertex.label).get, edges)
      })
      graphListBuffer += new Graph(graph.id, frequentVertices.toIndexedSeq)

    }
    (new GraphSet(graphListBuffer.toIndexedSeq), oneEdgeCounter)
  }

  def projectWithOneEdge(graphSet: GraphSet, edgeCode: EdgeCode) = {
    graphSet.graphSet.flatMap(g => {
      g.vertices.flatMap(v => {v.edges
        .filter(e => (e.fromLabel, e.edgeLabel, e.toLabel) == (edgeCode.fromLabel, edgeCode.edgeLabel, edgeCode.toLabel))
        .map(e => (g.id, Map(edgeCode.fromId -> e.fromId, edgeCode.toId -> e.toId)))
      })
    }).toList
  }

  /**
   * Remove specified edge code
   * @param graphSet
   * @param edgeCode
   * @return new graph set with specified edgecode removed
   */
  def shrink(graphSet: GraphSet, edgeCode: EdgeCode) = {
    def seen(edge: Edge, edgeCode: EdgeCode) = {
      val condition1 = (edge.fromLabel, edge.edgeLabel, edge.toLabel) == (edgeCode.fromLabel, edgeCode.edgeLabel, edgeCode.toLabel)
      val condition2 = (edge.toLabel, edge.edgeLabel, edge.fromLabel) == (edgeCode.fromLabel, edgeCode.edgeLabel, edgeCode.toLabel)
      condition1 || condition2
    }
    val graphList = graphSet.graphSet.map(g => {
      val vertices = g.vertices.map(v => {
        val edges = v.edges.filter(e => ! seen(e, edgeCode))
        new Vertex(v.id, v.label, edges)
      })
      new Graph(g.id, vertices)
    })
    new GraphSet(graphList)
  }

  /**
   * Find all possible backward growth
   * @param graph
   * @param mapping
   * @param dfsCode
   * @param rightMostPath
   * @return a list of possible backward growth
   */
  def findBackwardGrowth(graph: Graph, mapping: Map[Int, Int], dfsCode: DFSCode, rightMostPath: IndexedSeq[Int]) = {
    val bound = if (dfsCode.codes.last.fromId > dfsCode.codes.last.toId) dfsCode.codes.last.toId else -1
    val rightMostNode = rightMostPath.last

    val originalRightMostVertex = graph.findVertex(mapping.get(rightMostNode).get)

    rightMostPath.dropRight(2).filter(_ > bound).flatMap(nodeId => {
      val possibleVertexId = mapping.get(nodeId).get
      originalRightMostVertex.edges
                              .filter(_.toId == possibleVertexId)
                              .map(e => ((rightMostNode, nodeId, e.fromLabel, e.edgeLabel, e.toLabel), mapping))

    })
  }

  /**
   * Find all possible forward growth
   * @param graph
   * @param mapping
   * @param dfsCode
   * @param rightMostPath
   * @return a list of possible forward growth
   */
  def findForwardGrowth(graph: Graph, mapping: Map[Int, Int], dfsCode: DFSCode, rightMostPath: IndexedSeq[Int]) = {
    val rightMostNode = rightMostPath.last
    val mappedIds = mapping.values.toSet

    rightMostPath.flatMap(nodeId => {
      val originalVertex = graph.findVertex(mapping.get(nodeId).get)
      originalVertex.edges.filter(e => ! mappedIds.contains(e.toId))
                          .map(e => ((nodeId, rightMostNode + 1, e.fromLabel, e.edgeLabel, e.toLabel), mapping+((rightMostNode + 1, e.toId))))
    })
  }

  /**
   * Give the comparison result of two edge code
   * @param edgeCode1
   * @param edgeCode2
   * @return -1 for <, 0 for ==, 1 for >
   */
  def edgeCodeCompare(edgeCode1: (Int, Int, Int, Int, Int), edgeCode2: (Int, Int, Int, Int, Int)) = {
    val forward1 = edgeCode1._1 < edgeCode1._2
    val forward2 = edgeCode2._1 < edgeCode2._2
    if (forward1 && forward2) {
      if (edgeCode1._1 != edgeCode2._1) {
        edgeCode2._1 - edgeCode1._1       //edgeCode2 - edgeCode1 !
      } else if (edgeCode1._3 != edgeCode2._3) {
        edgeCode1._3 - edgeCode2._3
      } else if (edgeCode1._4 != edgeCode2._4) {
        edgeCode1._4 - edgeCode2._4
      } else if (edgeCode1._5 != edgeCode2._5) {
        edgeCode1._5 - edgeCode2._5
      } else {
        0
      }
    } else if (!forward1 && !forward2) {
      if (edgeCode1._2 != edgeCode2._2) {
        edgeCode1._2 - edgeCode2._2
      } else if (edgeCode1._4 != edgeCode2._4) {
        edgeCode1._4 - edgeCode2._4
      } else {
        0
      }

    } else {
      if (forward1) 1 else -1
    }
  }

  /**
   * The s == min(s) check in original paper
   * @param dfsCode
   */
  def isMinDFSCode(dfsCode:DFSCode):Boolean = {
    def search(queue: mutable.Queue[(List[Int], Map[Int, Int], Int, Int)], vertices: Array[Vertex]): Boolean = {
      while(queue.nonEmpty) {
        breakable {
          var (sequence, mapping, nextPosition, lastVisit) = queue.dequeue()
          val lastVertex = vertices(sequence.last)
          val backwardEdges = lastVertex.edges.filter(edge => (edge.toId != lastVisit) && mapping.contains(edge.toId))
            .map(edge => (mapping.get(edge.fromId).get, mapping.get(edge.toId).get, edge.fromLabel, edge.edgeLabel, edge.toLabel))
            .sortWith((e1, e2) => edgeCodeCompare(e1, e2) < 0)

          // Test backward edges
          for ((edgeCode, index) <- backwardEdges.zipWithIndex) {
            val result = edgeCodeCompare(edgeCode, dfsCode.codes(nextPosition + index).toTuple)
            if (result < 0) {
              return false
            } else if (result > 0) {
              break  // For continue
            }
          }

          // Update next position
          nextPosition += backwardEdges.size
          var forwardFlag = false

          // check forward edges
          breakable {
            while (sequence.nonEmpty) {
              val lastVertex = vertices(sequence.last)
              for (edge <- lastVertex.edges) {
                breakable {
                  if (!mapping.contains(edge.toId)) {
                    val newCode = (mapping(edge.fromId) , mapping.size, edge.fromLabel, edge.edgeLabel, edge.toLabel )
                    val result = edgeCodeCompare(newCode, dfsCode.codes(nextPosition).toTuple)
                    if (result < 0 ){
                      return false
                    } else if (result > 0) {
                      break() //continue
                    }
                    val updatedMapping = mapping + ((edge.toId, mapping.size))
                    val updatedSequence = sequence :+ edge.toId
                    queue.enqueue((updatedSequence, updatedMapping, nextPosition + 1, lastVertex.id))
                    forwardFlag = true
                  }
                }
              }

              if (forwardFlag) {
                break() // break out of the while loop
              } else {
                sequence = sequence.init
              }
            }
          }


        }
      }
      true
    }

    val tempVertices = Array.fill[Vertex](90)(null)
    var nodeNum = 0

    for(edgeCode <- dfsCode.codes) {
      val fromId = edgeCode.fromId
      val toId = edgeCode.toId
      val fromLabel = edgeCode.fromLabel
      val edgeLabel = edgeCode.edgeLabel
      val toLabel = edgeCode.toLabel

      val fromEdge = new Edge(fromId, toId, fromLabel, edgeLabel, toLabel)
      val toEdge = new Edge(toId, fromId, toLabel, edgeLabel, fromLabel)

      if (tempVertices(fromId) == null) {
        tempVertices(fromId) = new Vertex(fromId, fromLabel, List())
      }
      if (tempVertices(toId) == null) {
        tempVertices(toId) = new Vertex(toId, toLabel, List())
      }
      tempVertices(fromId).addEdge(fromEdge)
      tempVertices(toId).addEdge(toEdge)
      nodeNum = math.max(fromId + 1, math.max(toId + 1, nodeNum))

    }


    val queue = new mutable.Queue[(List[Int], Map[Int, Int], Int, Int)]
    val vertices = tempVertices.filter(_ != null).map(v => new Vertex(v.id, v.label, v.tempEdges.toList))
    vertices.foreach(v => queue.enqueue((List(v.id), Map(v.id -> 0), 0, -1))) // real id => edge code

    search(queue, vertices)

  }


  def subgraphMining(graphSet: GraphSet, s: ListBuffer[FinalDFSCode], dfsCode: DFSCode, minSupport: Int): Unit = {
    if (isMinDFSCode(dfsCode)) {

      val support = dfsCode.support
      if (support >= minSupport) {
        s += new FinalDFSCode(dfsCode.codes, support)
        //println(dfsCode.info)
        //println(support)
        val (childrenGraphSet, childrenCounting) = enumerateSubGraph(graphSet, dfsCode)

        val forwardMapping = dfsCode.codes.filter(ec => ec.fromId < ec.toId).map(ec => (ec.fromId, (ec.edgeLabel, ec.toLabel))).toMap

        val supportedChild = childrenCounting//.par
                                              .filter(_._2 >= minSupport)
                                              .keys
                                              //.filter(child => filterImpossible(child, forwardMapping))
                                              .toList
                                              .sortWith((e1, e2) => edgeCodeCompare(e1, e2) < 0)

        for(child <- supportedChild) {
          val edgeCode = new EdgeCode(child._1,child._2,child._3,child._4,child._5)
          val codes = dfsCode.codes :+ edgeCode
          val projectedGraphSet = childrenGraphSet.get(child).get.toList
          val extendedDFSCode = new DFSCode(codes, projectedGraphSet, childrenCounting.get(child).get)
          subgraphMining(graphSet, s, extendedDFSCode, minSupport)
        }
      }
    }

  }

  /**
   * Parallelized children enumeration
   * @param graphSet
   * @param dfsCode
   * @return possible children graph and corresponding counting
   */
  def enumerateSubGraph(graphSet: GraphSet, dfsCode: DFSCode) = {
    def aux(graphId: Int, mapping: Map[Int, Int], rightMostPath:IndexedSeq[Int]) = {
      val graph = graphSet.graphSet(graphId)
      val backwardGrowth = findBackwardGrowth(graph, mapping, dfsCode, rightMostPath)
      val forwardGrowth = findForwardGrowth(graph, mapping, dfsCode, rightMostPath)
      (graphId, forwardGrowth, backwardGrowth)
    }

    val rightMostPath = buildRightMostPath(dfsCode)

    val pGraphSet = dfsCode.graphSet.par
    val result  = pGraphSet.map(gs => aux(gs._1, gs._2, rightMostPath)).toList



    val childrenGraphSet = new mutable.HashMap[(Int, Int, Int, Int, Int), ListBuffer[(Int, Map[Int, Int])]]
    val graphIdSet = new mutable.HashMap[(Int, Int, Int, Int, Int), mutable.HashSet[Int]]

    for ((graphId, forwardGrowth, backwardGrowth) <- result) {
      for ((growth, mapping) <- backwardGrowth) {
        if (! childrenGraphSet.contains(growth)) {
          childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
          graphIdSet.put(growth, new mutable.HashSet[Int])
        }
        childrenGraphSet(growth) += ((graphId, mapping))
        graphIdSet(growth) += graphId
      }

      for ((growth, mapping) <- forwardGrowth) {
        if (! childrenGraphSet.contains(growth)) {
          childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
          graphIdSet.put(growth, new mutable.HashSet[Int])
        }
        childrenGraphSet(growth) += ((graphId, mapping))
        graphIdSet(growth) += graphId
      }
    }

    val childrenCount = graphIdSet.map(pair => (pair._1, pair._2.size))
    (childrenGraphSet, childrenCount)
  }

  def printResult(s: ListBuffer[FinalDFSCode], vertexLabelMapping: Map[Int, Int], edgeLabelMapping: Map[Int, Int], vertexLabelCounter: Map[Int, Int]) = {

    val backVertexLabelMapping = vertexLabelMapping.map {case (k,v) => (v, k)}
    val backEdgeLabelMapping = edgeLabelMapping.map { case (k,v) => (v, k)}

    val totalSize = s.size + vertexLabelMapping.size
    println(s"   ${totalSize} frequent subgraphs")
    println(s"=  ${s.size} frequent subgraphs with at least one edge")
    println(s"+  ${vertexLabelMapping.size} frequent vertices")
    s.zipWithIndex.foreach {case (code, index) => println(code.info(index, backVertexLabelMapping, backEdgeLabelMapping))}
    vertexLabelCounter.toList.zipWithIndex.foreach{ case ((original, count), index) => println(s"t # ${index + s.size} * ${count}\nv 0 ${original}")}
  }

} // object Algorithm

} //package gSpan