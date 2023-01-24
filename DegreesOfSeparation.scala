package com.graph
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import spark.implicits._

object DegreesOfSeparation{

    val spark:SparkSession = SparkSession.builder()
                                        .appName("test")
                                        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")


    def get_graph(vertices: RDD[(VertexId, String)], edges: RDD[Edge[String]]) : Graph[String, String]={
        val default = ("0")
        val graph = Graph(vertices, edges, default)
        return graph
    }

    def read_nodes(spark: SparkSession, path: String): RDD[(VertexId, String)]={
        val df = spark.read.json(path)
        // zip the nodes with their index and swap columns to get the id as 1st column
        val nodes : RDD[(VertexId, String)] = df.select(col("id")).map(r => r(0).toString).rdd.zipWithIndex.map(_.swap)
        return nodes
    }

    def read_edges(spark: SparkSession, path: String, nodes: RDD[(VertexId, String)]): RDD[Edge[String]]={
        val new_nodes = nodes.map{case (vid, aut) => (vid, aut, "reply")}
        // convert the nodes to a dataframe
        val vdf = new_nodes.toDF("id", "author", "action")
        val dfc = spark.read.json(path)
        // retrieve the edges as source, destination, and type of interaction
        val e1  = dfc.join(vdf, dfc("src") === vdf("author"))
                        .select(vdf("id") as "a_id", vdf("author") as "a", dfc("dst") as "pa", vdf("action") as "act")
        val e2 = e1.join(vdf, vdf("author") === e1("pa")).select(e1("a_id") as "src", vdf("id") as "dst", e1("act") as "meta")
        val e3 = e2.map(row => Edge(row.getAs[Long]("src"), row.getAs[Long]("dst"), row.getAs[String]("meta")))
        return e3.rdd
    }

    def get_short_path(graph: Graph[String, String]) : RDD[(VertexId, Float)]={
        val vert_ids = graph.vertices.map{case (x_id, x_map) => x_id}
        // take a random subset of the vertex ids and convert them to 
        // a sequence to input them into the ShortestPath algorithm
	    val sample : Seq[VertexId] = vert_ids.takeSample(false, 15).toSeq
	    println("Average shortest path for user IDs " + sample)
        val sp_result = ShortestPaths.run(graph, sample)
        // take out all empty maps
        var f_vert1 = sp_result.vertices.filter{case (x_id, x_map) => x_map.nonEmpty}
        var m_vert1 = f_vert1.map{case (x_id, x_map) => (x_id, x_map.foldLeft(0)(_+_._2), x_map.size)}
        // calculate the average shortest path
        var m_vert2 = m_vert1.map{case (x_id, map_sum, map_size) => (x_id, map_sum.toFloat/map_size)}
        var f_vert2 = m_vert2.filter{case (x_id, x_avg) => !(x_avg==0.0)}
        return f_vert2
    }


    def main(args : Array[String]) {
        // get the data for the first two months of 2013
        val nodes = read_nodes(spark, "/user/s3049221/reddit/nodes_2013-0[12]")
        val edges = read_edges(spark, "/user/s3049221/reddit/edges_2013-0[12]", nodes)
        // print out the number of nodes and edges
        println("Nodes: ")
        println(nodes.count())
        println("Edges: ")
        println(edges.count())
        // run the functions to get the graph and shortest paths
        val graph = get_graph(nodes, edges)
        var s_paths = get_short_path(graph)
        // map to only the average shortest paths
	    var paths = s_paths.map{case (a, b) => b}
        // take the average of all average shortest paths
        println("Average degrees of separation: ")
	    var mn = paths.mean()
	    println(mn)
        // print out a sample of 10 results
        println("Separation degree with 10 users: ")
	    s_paths.take(10).foreach(println)
    }
}
