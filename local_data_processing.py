import subprocess
import glob
import pandas as pd
import json
import networkx as nx
import numpy as np
import datetime


def unix_to_date(timestamp: int) -> str:
    date = datetime.datetime.fromtimestamp(timestamp)
    # Return the formatted date string
    return date.strftime("%Y-%m-%d")


# Requires zstd package to be installed
def decompress_zst(in_path: str, out_path: str):
    for f in glob.glob(f'{in_path}/*.zst'):
        # name format: reddit-yyyy-nn
        output = out_path + '/reddit_' + f.split('_')[1].split('.')[0]

        # decompress
        print(f'Decompressing: {f}')
        subprocess.run(['unzstd', f, '--memory=2048MB', '-o', output])


def concat_files(files: list) -> pd.DataFrame:
    '''
        Read and concatenate a number of json line files 
        into a single dataframe
    '''

    frames = []
    for file in files:
        data = []
        with open(file) as f:
            for line in f:
                data.append(json.loads(line))

        frame = pd.DataFrame(data)
        frames.append(frame)

    df = pd.concat(frames, ignore_index=True)

    del data
    del frames

    return df


def get_edge_weights(df: pd.DataFrame, by_date=False) -> pd.DataFrame:
    '''
        Returns the number of interactions between two authors
    '''
    edges = df \
        .groupby(["author", "parent_author"]) \
        .size() \
        .reset_index(name='weight') \
        .sort_values('weight', ascending=False)

    edges = edges[edges['weight'] > 1]

    # timestamps = df \
    #         .groupby(["author", "parent_author", 'created_utc']) \
    #         .size() \
    #         .reset_index(name="weight") \
    #         .sort_values('weight', ascending=False)

    # res = pd.merge(timestamps, edges,  how='left', left_on=['author','parent_author'], right_on=['author','parent_author'])
    # res.drop('weight_y', axis=1, inplace=True)

    return edges


def add_comments_to_edges(edge_list: pd.DataFrame, comment_list: pd.DataFrame) -> pd.DataFrame:
    '''
        Append comments to edge list for matching author connections
    '''
    pass


def get_subgraphs(graph: nx.Graph):
    '''
        Return a list of SSC's within a given graph
    '''
    return sorted(nx.strongly_connected_components(graph), key=len, reverse=True)


def get_shortest_paths(graph):
    '''
        Calculate the min, max and average shortest path 
        (deg of separation) in a graph
    '''

    shortest_paths = []
    for k in graph:
        # print(f"calculating shortest paths for node: {k}")
        for l in graph:
            if k != l:
                sl = nx.shortest_path_length(graph, k, l)
                shortest_paths.append(sl)

    print("Minimum SPL: ", min(shortest_paths))
    print("maximum SPL: ", max(shortest_paths))
    print("average SPL is", np.average(shortest_paths))


# net = Network(filter_menu=True, select_menu=True, directed=True)
# net.from_nx(G)
# net.show('test.html')


def preprocess_frame(df):
    '''
        Preprocess dataframe by removing 
        - unwanted columns
        - comments from deleted users
        - splitting parent id from comment type
        - matching comments with parent author
        - removing comments for which no matching parent author is available
    '''

    res = df.drop([
        'author_flair_css_class',
        'stickied',
        'retrieved_on',
        'author_flair_text',
        'distinguished'
    ], axis=1)

    res = res[res.author != '[deleted]']

    res[['parent_type', 'parent_id']] = res.parent_id.str.split('_', expand=True)

    parent_map = df.set_index('id')['author']
    res['parent_author'] = res['parent_id'].map(parent_map)
    res.dropna(inplace=True, subset=['parent_author'])
    return res


# file = [glob.glob('./data/2016/*')[2]]
# df = concat_files(file)
# df = preprocess_frame(df)
#
# edges = get_edge_weights(df)
# print(edges.head(50))

path_out = "D:\MSc_DST\Year1\Q2\BigData\project\data\data2"
path_in = "D:\MSc_DST\Year1\Q2\BigData\project\data"

decompress_zst(path_in, path_out)

# df.to_csv('reddit-2016-full.csv', encoding='utf-8')

# df = pd.read_csv('reddit-2016-full.csv')

# df['created_utc'] = df['created_utc'].map(unix_to_date)

# edge_frame = get_edge_weights(df)
# G = nx.from_pandas_edgelist(df, 'author', 'parent_author', 'created_utc', create_using=nx.DiGraph())
# net = Network(filter_menu=True, select_menu=True, directed=True)
# net.from_nx(G)
# net.show('test.html')

# for num_comments in range(2,16):
#     print(f'num_comments > {num_comments}')
#     df.sort_values(by='created_utc')
#     G = nx.from_pandas_edgelist(df, 'author', 'parent_author', 'created_utc', create_using=nx.DiGraph())
#     net = Network(filter_menu=True, select_menu=True, directed=True)
#     net.from_nx(G)
#     net.show('test.html')
#     exit()
# subgraphs = sorted(nx.strongly_connected_components(G), key=len, reverse=True)
# #print(f'largest subgraph: {len(subgraphs[0])}')
# #print(f'smallest subgraph: {len(subgraphs[-1])}')
# get_shortest_paths(subgraphs[0])
# print('----')
