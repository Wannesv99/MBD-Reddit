from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)


from graphframes import *

# local file for testing
df = spark.read.json('file:///home/s1943251/mbd-project/data/reddit_2006-01')

def ceildiv(a, b):
    return -(a // -b)


def get_edge_weights(df):
    '''
    Not sure if we need this since the weight should automatically be calculated in the graph
    '''
    return df.groupBy(["author", "parent_author"]).count().orderBy('count', ascending=False)


def clean_dataframe(df):
    '''
        Clean comment dataframe by removing 
        - unwanted columns
        - comments from deleted users
        - splitting parent id from comment type
        - matching comments with parent author
        - removing comments for which no matching parent author is available
    '''
    res = df.drop(
            'author_flair_css_class', 
            'stickied', 
            'retrieved_on', 
            'author_flair_text',
            'distinguished'
            )

    res = res.filter(res.author != '[deleted]')
 
    # t1_12345 -> t1, 12345 (type, id).
    split_col = split(res['parent_id'], '_')
    res = res.withColumn('parent_type', split_col.getItem(0)) \
             .withColumn('parent_id', split_col.getItem(1)) 

    # Join the child comment with the matching parent comment and select parent author.
    res = res.alias("A").join(res.alias("B"), col("A.parent_id") == col("B.id"),"left")\
        .select('A.author', 'A.body', 'A.created_utc', 'A.id', 'A.link_id', 'A.parent_id', 
                'A.parent_type', 'A.subreddit', 'A.subreddit_id', 'A.score', 'A.ups',
                col("B.author").alias("parent_author")
        )
    
    # take out comments where parent comment is null.
    res = res.filter(res.parent_author.isNotNull())
    
    return res


def get_node_frame(df, author_column_name='author', parent_column_name='parent_author'):
    '''
        Transform datafame into format needed for node dataframe in GraphFrame object
    '''
    authors = df.select(col(author_column_name).alias('id'))
    parents = df.select(parent_column_name)

    nodes = authors.union(parents).distinct()

    return nodes
    

def get_edge_frame(df, source_col='author', destination_col='parent_author'):
    '''
        Transform dataframe into format needed for edge dataframe in GraphFrame object
    '''

    edges = df.select(col(source_col).alias('src'), 
                    col(destination_col).alias('dst'),
                    col('body').alias('comment'))

    return edges



df = clean_dataframe(df)

edges = get_edge_frame(df)
nodes = get_node_frame(df)


graph = GraphFrame(nodes, edges)


graph = graph.dropIsolatedVertices()



result = graph.connectedComponents().show()

#result = graph.pageRank(maxIter=10).vertices.show()
# spark-submit --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11 --deploy-mode cluster --master yarn --conf spark.dynamicAllocation.maxExecutors=10 main.py
#result = graph.stronglyConnectedComponents(maxIter=1)

# with open('test.txt', 'w') as sys.stdout:
#     print('we got here')
    #result.select("id", "component").orderBy("component").show()

# num_nodes = graph.vertices.count()
# split_size = 100
# num_splits = ceildiv(num_nodes, split_size)

# copy_nodes = graph.vertices

# for n in range(num_splits):
#     temp_nodes = copy_nodes.limit(split_size)
#     copy_nodes = copy_nodes.subtract(temp_nodes)

#     temp_id_list = temp_nodes.select("id").rdd.flatMap(lambda x: x).collect()
#     print(temp_id_list)
#     temp_shortest_paths = graph.shortestPaths(landmarks=temp_id_list[0])
#     temp_shortest_paths.show()
#     print(temp_shortest_paths.count())

#     break