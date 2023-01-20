package com.graph
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._


object BFS{

  val spark:SparkSession = SparkSession.builder()
    .appName("test")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("OFF")


  // val sc:sparkContext = sparkContext

  /**
   * Returns the shortest directed-edge path from src to dst in the graph. If no path exists, returns
   * the empty list.
   */
  def bfs[VD, ED](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
    if (src == dst) return List(src)

    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
    var g: Graph[(Int, VertexId), ED] =
      graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()

    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](
        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
        },
        (a, b) => if (a._1 < b._1) a else b).cache()

      if (msgs.count == 0) return List.empty

      g = g.ops.joinVertices(msgs) {
        (id, oldAttr, newAttr) =>
          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()

      dstAttr = g.vertices.filter(_._1 == dst).first()._2
    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

    println(path)

    path
  }


  def main(args : Array[String]) {
    val g = Graph.fromEdgeTuples(
      spark.sparkContext.parallelize(List((1L, 2L), (2L, 3L), (3L, 4L), (2L, 4L), (10L, 11L))),
      defaultValue = 1)

    bfs(g, 1L, 4L)

    bfs(g, 2L, 4L)

    bfs(g, 1L, 10L)

    bfs(g, 4L, 3L)

    bfs(g, 3L, 3L)
  }
}