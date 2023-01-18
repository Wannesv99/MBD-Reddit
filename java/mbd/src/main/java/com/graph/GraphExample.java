package com.graph;
import org.apache.spark.api.java.*;
import org.apache.spark.graphx.*;

import org.apache.spark.storage.StorageLevel;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import org.apache.spark.SparkConf;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.graphx.lib.*;

import static scala.collection.Seq.*;
import scala.collection.JavaConversions;
public class GraphExample {
    public static void main(String[] args) {

        // System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);


        List<Edge<String>> edges = new ArrayList<>();

        edges.add(new Edge<String>(1, 2, "Friend1"));
        edges.add(new Edge<String>(2, 3, "Friend2"));
        edges.add(new Edge<String>(1, 3, "Friend3"));
        edges.add(new Edge<String>(4, 3, "Friend4"));
        edges.add(new Edge<String>(4, 5, "Friend5"));
        edges.add(new Edge<String>(2, 5, "Friend6"));


        JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(edges);


        Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        System.out.print("################          ALL GOOD          ###################");
        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        // List<Tuple2<Object,String>> verts = graph.vertices().toJavaRDD().collect();

        // verts.forEach(map -> map.forEach((k,v) -> {}));
        // System.out.println(k,v)
//}));

        Seq test = (Seq) JavaConversions.asScalaBuffer(edges);



        Graph<Map<Object, Object>, String> shortestPaths = ShortestPaths.run(graph, test, null);

        shortestPaths.vertices().toJavaRDD().collect().forEach(System.out::println);


        javaSparkContext.close();

        // sc.stop();
    }
}