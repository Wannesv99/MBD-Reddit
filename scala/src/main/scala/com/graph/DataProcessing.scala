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

        //t1_12345 -> t1, 12345 (type, id)
        val split_col = split(df2("parent_id"), "_")
        df2 = df2.withColumn("parent_type", split_col(0))
                .withColumn("parent_id", split_col(1))

        // df2 = df2.select(split(col("parent_id"), "_").getItem(0).as("parent_type"),
        //             split(col("parent_id"), "_").getItem(1).as("parent_id"))


        // var joined = df2.as("df11").join(df2.as("df22"), $"df11.parent_id" === $"df22.id", "left").select($"df11.*", $"df22.author".as("parent_author"))


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

        // .join(dfs.as("df22"), $"df11.parent_id" === $"df22.id", "left")
        // .withColumn("parent_authors", when($"df11.parent_id" === $"df22.id", $"df22.author")
        // .otherwise($"df11.parent_author")).select($"df11.author", $"df11.body", $"df11.created_utc", 
        // $"df11.id", $"df11.link_id", $"df11.parent_id", $"df11.parent_type", $"df11.subreddit", 
        // $"df11.subreddit_id", $"df11.score",$"parent_authors")

        return joined
    }

    /// cols = Seq of cols that will be removed from comments df
    def get_final_comm_df(dfc:DataFrame, dfs:DataFrame, cols: Seq[String]) :DataFrame={

        var dfc2 = comments_clean_dataframe(dfc, cols)
        var dfs2 = posts_clean_dataframe(dfs)

        var joined = link_comments_to_posts(dfc2, dfs2)

        joined = joined.filter(joined("parent_author").isNotNull)
        joined = joined.filter(joined("parent_author") === "[deleted]")

        return joined
    }


    def get_vertex_edge_rdd(dfc: DataFrame): (RDD[(VertexId, String)], RDD[Edge[String]])={

        // VertexId is just an alias for Long

        // creates rdd of [id, (id, author)]; the first id is generated fby zipWithIndex
        // val vertices : RDD[(VertexId, (String,String))] = dfc.
        //                                                 select(col("id"), col("author"))
        //                                                 .rdd
        //                                                 .map(x => (x.get(0).toString, x.get(1).toString))
        //                                                 .zipWithIndex
        //                                                 .map(_.swap)
        
        // val edges : RDD[(VertexId, String)] = dfcc.select(explode(array(col("id"), col("author")))).distinct.rdd.map(_.getAs[String](0)).zipWithIndex.map(_.swap)

        //  val vdf = vertices.map{case (key, value) => (key, value._1, value._2)}.toDF("id", "index", "author")

        val vertices : RDD[(VertexId, String)] = dfc
                                                .select(col("author"))
                                                .map(r => r(0).toString)
                                                .rdd
                                                .zipWithIndex
                                                .map(_.swap)

        val vdf = vertices.toDF("id", "author")
       


        //TODO!!!
        val edges : RDD[Edge[String]] = dfc
                                        .join(vdf, dfc("author") === vdf("author")).select(vdf("id") as "src", vdf("author") as "auth", dfc("parent_author"))
                                        .join(vdf, vdf("author") === col("parent_author"))
                                        .select(col("src"), vdf("id") as "dst")
                                        .withColumn("meta", lit("reply"))
                                        .rdd
                                        .map(row => Edge(row.getAs[Long]("src"), row.getAs[Long]("dst"), row.getAs[String]("meta")))


        return (vertices, edges)

    }


    def get_graph(vertices: RDD[(VertexId, String)], edges: RDD[Edge[String]]) :Graph[String, String]={

        val default = ("0")

        val graph = Graph(vertices, edges, default)

        return graph
    }

    


    def main(args : Array[String]) {

        
        // val dfc = read_data(spark, "/user/s3049221/reddit/RC_2006-11.json")
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

        df_fin.show()

        val (vertices, edges) = get_vertex_edge_rdd(df_fin)


        val graph = get_graph(vertices, edges)
        
        graph.inDegrees.take(5)
    }


}