package com.graph
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

object DataProcessing{

    val spark:SparkSession = SparkSession.builder()
                                        .appName("test")
                                        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("OFF")


    def read_data(spark: SparkSession, path: String): DataFrame={
        val df = spark.read.json(path)
        return df
    }

    def remove_cols(df: DataFrame, cols: Seq[String]): DataFrame={
        val df2 = df.drop(cols:_*)
        return df2
    }


    def comments_clean_dataframe(df: DataFrame, cols:Seq[String]):DataFrame={
        var df2 = remove_cols(df, cols)
        df2 = df2.filter(!(df2("author") === "[deleted]"))
        val split_col = split(df2("parent_id"), "_")
        df2 = df2.withColumn("parent_type", split_col(0))
                .withColumn("parent_id", split_col(1))

        var joined = df2.as("df11")
                    .join(df2.as("df22"), col("df11.parent_id") === col("df22.id"), "left")
                    .select($"df11.*", col("df22.author").as("parent_author"))
        return joined
    }


    def posts_clean_dataframe(df: DataFrame) :DataFrame ={
        var df2 = df.select(
                "author", 
                "id", 
                "num_comments", 
                "subreddit", 
                "subreddit_id", 
                "title" 
                )
        df2 = df2.filter(df2("author") === "[deleted]")
        df2 = df2.filter(df2("num_comments") > 0)
        return df2
    }


    def link_comments_to_posts(dfc: DataFrame, dfs: DataFrame) :DataFrame ={
        var joined = dfc.as("df11").join(dfs.as("df22"), col("df11.parent_id") === col("df22.id"), "left")
            .withColumn("parent_author", when(col("df11.parent_id") === col("df22.id"), col("df22.author"))
            .otherwise(col("df11.parent_author")))
            .select(col("df11.author"), col("df11.body"), col("df11.created_utc"), 
            col("df11.id"), col("df11.link_id"), col("df11.parent_id"), col("df11.parent_type"), col("df11.subreddit"), 
            col("df11.subreddit_id"), col("df11.score"),col("parent_author"))
        return joined
    }


    def get_final_comm_df(dfc:DataFrame, dfs:DataFrame, cols: Seq[String]) :DataFrame={
        var dfc2 = comments_clean_dataframe(dfc, cols)
        var dfs2 = posts_clean_dataframe(dfs)
        var joined = link_comments_to_posts(dfc2, dfs2)
        joined = joined.filter(joined("parent_author").isNotNull)
        joined = joined.filter(!(joined("parent_author") === "[deleted]"))
        return joined
    }


    def get_node_frame(dfc: DataFrame): RDD[(VertexId, String)]={
        val nodes : RDD[(VertexId, String)] = dfc.select(col("author")).map(r => r(0).toString).rdd.zipWithIndex.map(_.swap)
        return nodes 
    }


    def get_edge_frame(dfc: DataFrame, nodes: RDD[(VertexId, String)]): RDD[Edge[String]]={
        val vdf = nodes.toDF("id", "author")
	    val e1 = dfc.join(vdf, dfc("author") === vdf("author"))
					.select(vdf("id") as "a_id", vdf("author") as "a", dfc("parent_author") as "pa",
						dfc("body") as "bod")
	    val e2 = e1.join(vdf, vdf("author") === e1("pa")).select(e1("a_id") as "src", vdf("id") as "dst", e1("bod") as "meta")
	    val e3 = e2.map(row => Edge(row.getAs[Long]("src"), row.getAs[Long]("dst"), row.getAs[String]("meta")))
        return e3.rdd
    }


    def get_graph(vertices: RDD[(VertexId, String)], edges: RDD[Edge[String]]) : Graph[String, String]={
        val default = ("0")
        val graph = Graph(vertices, edges, default)
        return graph
    }

    def read_nodes(spark: SparkSession, path: String): RDD[(VertexId, String)]={
        val df = spark.read.json(path)
        val nodes : RDD[(VertexId, String)] = df.select(col("id")).map(r => r(0).toString).rdd.zipWithIndex.map(_.swap)
        return nodes
    }

    def read_edges(spark: SparkSession, path: String, nodes: RDD[(VertexId, String)]): RDD[Edge[String]]={
        val new_nodes = nodes.map{case (vid, aut) => (vid, aut, "reply")}
        val vdf = new_nodes.toDF("id", "author", "action")
        val dfc = spark.read.json(path)
        val e1  = dfc.join(vdf, dfc("src") === vdf("author"))
                        .select(vdf("id") as "a_id", vdf("author") as "a", dfc("dst") as "pa", vdf("action") as "act")
        val e2 = e1.join(vdf, vdf("author") === e1("pa")).select(e1("a_id") as "src", vdf("id") as "dst", e1("act") as "meta")
        val e3 = e2.map(row => Edge(row.getAs[Long]("src"), row.getAs[Long]("dst"), row.getAs[String]("meta")))
        return e3.rdd
    }

    def get_short_path(graph: Graph[String, String]) : RDD[(VertexId, Float)]={
        val vert_ids = graph.vertices.map{case (x_id, x_map) => x_id}
        // take a subset of the vertex ids and convert them to a sequence
        // to input them into the ShortestPath algorithm
        val sub_seq = vert_ids.take(5).toSeq
        val sp_result = ShortestPaths.run(graph, sub_seq)
        // take out all empty maps
        var f_vert1 = sp_result.vertices.filter{case (x_id, x_map) => x_map.nonEmpty}
        var m_vert1 = f_vert1.map{case (x_id, x_map) => (x_id, x_map.foldLeft(0)(_+_._2), x_map.size)}
        // calculate the average shortest path
        var m_vert2 = m_vert1.map{case (x_id, map_sum, map_size) => (x_id, map_sum.toFloat/map_size)}
        var f_vert2 = m_vert2.filter{case (x_id, x_avg) => !(x_avg==0.0)}
        return f_vert2
    }


    def main(args : Array[String]) {
        val nodes = read_nodes(spark, "/user/s3049221/reddit/nodes_2013-01")
        val edges = read_edges(spark, "/user/s3049221/reddit/edges_2013-01", nodes)
        //val dfc = read_data(spark, "/user/s3049221/reddit/RC_2013-0*.json")
        //println("Comments: ")
        //println(dfc.count())
        //val dfs = read_data(spark, "/user/s3049221/reddit/RS_2013-0*.json")
        //println("Posts: ")
        //println(dfs.count())
        //val cols = Seq("author_flair_css_class", 
        //               "stickied", 
        //               "retrieved_on", 
        //               "author_flair_text",
        //               "distinguished",
        //               "edited",
        //               "gilded",
        //               "ups")
        //val df_fin = get_final_comm_df(dfc, dfs, cols)
        //val nodes = get_node_frame(df_fin)
        //val edges = get_edge_frame(df_fin, nodes)
        println("Nodes: ")
        println(nodes.count())
        println("Edges: ")
        println(edges.count())
        val graph = get_graph(nodes, edges)
        var s_paths = get_short_path(graph)
	    //val v_ids = graph.vertices.map{case (a, b) => a}	
	    //val subgraph = v_ids.take(5).toSeq
        //val sp_result = ShortestPaths.run(graph, subgraph)
	    //var filtered = sp_result.vertices.filter{case (a, b) => b.nonEmpty}
	    //var map1 = filtered.map{case (x, b) => (x, b.foldLeft(0)(_+_._2), b.size)}
	    //var map2 = map1.map{case (a, b, c) => (a, b.toFloat/c)}.filter{case (a, b) => !(b==0.0)}
	    var paths = s_paths.map{case (a, b) => b}
	    var mn = paths.mean()
	    println(mn)
	    s_paths.take(3).foreach(println)
    }
}
