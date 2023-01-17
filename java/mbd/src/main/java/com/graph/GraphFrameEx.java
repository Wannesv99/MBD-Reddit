package com.graph;
import com.temps.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

public class GraphFrameEx {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkGraphFrames")
                .setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // javaSparkContext.addJar("/home/s3072347/project/gf_jar/graphframes-0.8.0-spark2.4-s_2.11.jar");

        SparkSession session = SparkSession.builder()
                .appName("SparkGraphFrameSample")
                // .config("spark.sql.warehouse.dir", "/file:C:/temp")
                .sparkContext(javaSparkContext.sc())
                .master("local[*]")
                .getOrCreate();


        List<User> users = new ArrayList<>();
        users.add(new User(1L, "John"));
        users.add(new User(2L, "Martin"));
        users.add(new User(3L, "Peter"));
        users.add(new User(4L, "Alicia"));

        List<Relationship> relationships = new ArrayList<>();
        relationships.add(new Relationship("Friend", "1", "2"));
        relationships.add(new Relationship("Following", "1", "4"));
        relationships.add(new Relationship("Friend", "2", "4"));
        relationships.add(new Relationship("Relative", "3", "1"));
        relationships.add(new Relationship("Relative", "3", "4"));

        Dataset<Row> userDataset = session.createDataFrame(users, User.class);
        Dataset<Row> relationshipDataset = session.createDataFrame(relationships, Relationship.class);

        GraphFrame graph = new GraphFrame(userDataset, relationshipDataset);

        graph.vertices().show();
        graph.edges().show();


        javaSparkContext.close();
    }
}
