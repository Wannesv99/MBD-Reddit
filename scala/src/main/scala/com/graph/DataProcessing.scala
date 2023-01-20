package com.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import java.io._
import java.nio.file.{Files, Paths}


object DataProcessing {

  val spark: SparkSession = SparkSession.builder()
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("OFF")

  def read_data(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.json(path)
    return df
  }

  def remove_cols(df: DataFrame, cols: Seq[String]): DataFrame = {
    val df2 = df.drop(cols: _*)
    return df2
  }


  def comments_clean_dataframe(df: DataFrame, cols: Seq[String]): DataFrame = {
    // var df2 = remove_cols(df, cols)
    var df2 = df.select(
      "author",
      "parent_id",
      "id",
      "link_id"
    )

    df2 = df2.filter(!(df2("author") === "[deleted]"))
    val split_col = split(df2("parent_id"), "_")
    df2 = df2.withColumn("parent_type", split_col(0))
      .withColumn("parent_id", split_col(1))

    var joined = df2.as("df11")
      .join(df2.as("df22"), col("df11.parent_id") === col("df22.id"), "left")
      .select($"df11.*", col("df22.author").as("parent_author"))
    return joined
  }


  def posts_clean_dataframe(df: DataFrame): DataFrame = {
    var df2 = df.select(
      "author",
      "id",
      "num_comments"
    )
    df2 = df2.filter(df2("author") === "[deleted]")
    df2 = df2.filter(df2("num_comments") > 0)
    return df2
  }


  def link_comments_to_posts(dfc: DataFrame, dfs: DataFrame): DataFrame = {
    var joined = dfc.as("df11").join(dfs.as("df22"), col("df11.parent_id") === col("df22.id"), "left")
      .withColumn("parent_author", when(col("df11.parent_id") === col("df22.id"), col("df22.author"))
        .otherwise(col("df11.parent_author")))
      .select(col("df11.author"), col("df11.body"), col("df11.created_utc"),
        col("df11.id"), col("df11.link_id"), col("df11.parent_id"), col("df11.parent_type"), col("df11.subreddit"),
        col("df11.subreddit_id"), col("df11.score"), col("parent_author"))
    return joined
  }


  def get_final_comm_df(dfc: DataFrame, dfs: DataFrame, cols: Seq[String]): DataFrame = {
    var dfc2 = comments_clean_dataframe(dfc, cols)
    var dfs2 = posts_clean_dataframe(dfs)
    var joined = link_comments_to_posts(dfc2, dfs2)
    joined = joined.filter(joined("parent_author").isNotNull)
      .filter(!(joined("parent_author") === "[deleted]"))
      .filter(!(joined("parent_author") === joined("author")))

    return joined
  }


  def get_node_frame(dfc: DataFrame): RDD[(VertexId, String)] = {
    val nodes: RDD[(VertexId, String)] = dfc.select(col("author")).map(r => r(0).toString).rdd.zipWithIndex.map(_.swap)
    return nodes
  }


  def get_edge_frame(dfc: DataFrame, nodes: RDD[(VertexId, String)]): RDD[Edge[String]] = {
    val vdf = nodes.toDF("id", "author")
    val e1 = dfc.join(vdf, dfc("author") === vdf("author"))
      .select(vdf("id") as "a_id", vdf("author") as "a", dfc("parent_author") as "pa",
        dfc("body") as "bod")
    val e2 = e1.join(vdf, vdf("author") === e1("pa")).select(e1("a_id") as "src", vdf("id") as "dst").withColumn("meta", lit(1))
    val e3 = e2.map(row => Edge(row.getAs[Long]("src"), row.getAs[Long]("dst"), row.getAs[String]("meta")))
    return e3.rdd
  }


  def get_graph(vertices: RDD[(VertexId, String)], edges: RDD[Edge[String]]): Graph[String, String] = {
    val default = ("0")
    val graph = Graph(vertices, edges, default)
    return graph
  }


  def toGexf[VD, ED](g: Graph[String, String]): String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + "" +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }

  // creates a .gefx file that can be used on https://doc.linkurious.com/ogma/latest/
  // to (badly) visualize the graph
  def graph_to_file(g: Graph[String, String]) = {
    // val text = toGexf(g)

    // val out = new FileOutputStream(to); //Write data to a file
    // val buffer = new byte[4096];
    // val pw = new PrintWriter(new File("Gef.xml" ))
    // pw.write(text)
    // pw.close

    val filename = "Gef.gefx"

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(toGexf(g))
    bw.close()
  }


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


  def dijkstra[VD](g: Graph[VD, Double], origin: VertexId): Graph[(Int, Double), Int] = {
    var g2 = g.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

    for (i <- 1L to g.vertices.count - 1) {
      val currentVertexId =
        g2.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1

      val newDistances = g2.aggregateMessages[Double](
        ctx => if (ctx.srcId == currentVertexId)
          ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
        (a, b) => math.min(a, b))

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
        (vd._1 || vid == currentVertexId,
          math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
    }

    return g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue))._2))
  }


  def main(args: Array[String]) {
    val dfc = read_data(spark, "/user/s3072347/reddit_data/data/RC_*.json")
    println("Comments: ")
    println(dfc.count())
    val dfs = read_data(spark, "/user/s3072347/reddit_data/data/RS_*.json")
    println("Posts: ")
    println(dfs.count())
    val cols = Seq("author_flair_css_class",
      "stickied",
      "retrieved_on",
      "author_flair_text",
      "distinguished",
      "edited",
      "gilded",
      "ups")
    val df_fin = get_final_comm_df(dfc, dfs, cols)

    val nodes = get_node_frame(df_fin)
    val edges = get_edge_frame(df_fin, nodes)

    val graph = get_graph(nodes, edges)

    // graph_to_file(graph)

    val sp_result = ShortestPaths.run(graph, Seq(1, 6, 8))
    println("Result shortest path (TEST): ")
    var bla = sp_result.vertices.map { case (x, b) => (x, b.values) }
    // bla.take(5).foreach(println)


    val g3 = dijkstra(graph, 1L)

    g3.vertices.foreach(println)


  }
}