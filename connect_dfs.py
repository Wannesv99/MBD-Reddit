from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *


def comments_clean_dataframe(df):
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
        'distinguished',
        'edited',
        'gilded',
        'ups'
    )

    res = res.filter(res.author != '[deleted]')

    # t1_12345 -> t1, 12345 (type, id).
    split_col = split(res['parent_id'], '_')
    res = res.withColumn('parent_type', split_col.getItem(0)) \
        .withColumn('parent_id', split_col.getItem(1))

    # Join the child comment with the matching parent comment and select parent author.
    res = res.alias("A").join(res.alias("B"), col("A.parent_id") == col("B.id"), "left") \
        .select('A.author', 'A.body', 'A.created_utc', 'A.id', 'A.link_id', 'A.parent_id',
                'A.parent_type', 'A.subreddit', 'A.subreddit_id', 'A.score',  # 'A.ups',
                col("B.author").alias("parent_author")
                )

    # take out comments where parent comment is null.
    # res = res.filter(res.parent_author.isNotNull())

    return res


def posts_clean_dataframe(df):
    '''
        Clean posts dataframe by removing
        - unwanted columns
        - posts from deleted users
        - splitting parent id from comment type
        - matching comments with parent author
        - removing comments for which no matching parent author is available
    '''

    res = df.select(
        'author',
        'id',
        'num_comments',
        'subreddit',
        'subreddit_id',
        'title'
    )

    res = res.filter(res.num_comments > 0)
    # res = res.drop('num_comments')
    return res


### df for comments (dfc) and df for submissions (dfs)
def link_comments_to_posts(dfc, dfs):
    # Join the child comment with the matching parent submission and select parent author.
    res = dfc.alias("A").join(dfs.alias("B"), col("A.parent_id") == col("B.id"), "left") \
        .select('A.author', 'A.body', 'A.created_utc', 'A.id', 'A.link_id', 'A.parent_id',
                'A.parent_type', 'A.subreddit', 'A.subreddit_id', 'A.score',  # 'A.ups',
                col("B.author").alias("parent_author")
                )

    return res


spark = (
    SparkSession
    .builder
    .appName('GraphFrames_Test')
    .getOrCreate()
)

sqlContext = SQLContext(spark)

dfc = spark.read.json('reddit_data/data/RC_2006-02.json')
dfs = spark.read.json('reddit_data/data/RS_2006-02.json')

dfc = comments_clean_dataframe(dfc)
dfs = posts_clean_dataframe(dfs)

dfc = link_comments_to_posts(dfc, dfs)

print('############## Count b4 removing null parent_author: ' + str(dfc.count()))

dfc = dfc.filter(dfc.parent_author.isNotNull())

dfc.show()
print('############## Count after removing null parent_author: ' + str(dfc.count()))

