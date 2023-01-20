package com.graph
import org.apache.spark.sql.SparkSession
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


    def main(args : Array[String]) {
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
        df_fin.printSchema()
        println("Final: ")
        println(df_fin.count())
        //df_fin.show()
        val nodes = get_node_frame(df_fin)
        val edges = get_edge_frame(df_fin, nodes)
        println("Number of edges: ")
	    println(edges.count())
        val graph = get_graph(nodes, edges)
	val graph2 = graph.vertices.repartition(100)
	println(graph2.getNumPartitions)
        val sp_result = ShortestPaths.run(graph, Seq(1, 6, 8))
        println("Result shortest path (TEST): ")
	var bla = sp_result.vertices.map{case (x, b) => (x, b.values.sum/b.values.size)}
	bla.take(5).foreach(println)
	//sp_result.vertices.take(20).foreach(println)
    }
}
