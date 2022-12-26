import subprocess
import glob
import pandas as pd
import json
import networkx as nx
from pyvis.network import Network
import numpy as np


# Requires zstd package to be installed
def decompress_zst(in_path, out_path):
    for f in glob.glob(f'{in_path}/*.zst'):
        # name format: reddit-yyyy-nn
        output = out_path + '/reddit_' + f.split('_')[1].split('.')[0]
        
        # decompress
        print(f'Decompressing: {f}')
        subprocess.run(['unzstd', f, '--memory=2048MB', '-o', output])


def concat_files(files):
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


def get_edge_weights(df, sort=True):
    res = df \
        .groupby(["author", "parent_author"]) \
        .size() \
        .reset_index(name="weight")
    if sort:
        res.sort_values('weight', ascending=False, inplace=True)
    return res


def get_subgraphs(graph):
    return sorted(nx.strongly_connected_components(graph), key=len, reverse=True)


def get_shortest_paths(graph):
    shortest_paths= []
    for k in graph:
        # print(f"calculating shortest paths for node: {k}")
        for l in graph:
            if k!=l:
                sl=nx.shortest_path_length(G,k,l)
                shortest_paths.append(sl)

    print("Minimum SPL: ", min(shortest_paths))
    print("maximum SPL: ", max(shortest_paths))
    print("average SPL is", np.average(shortest_paths))


# net = Network(filter_menu=True, select_menu=True, directed=True)
# net.from_nx(G)
# net.show('test.html')




# Drop some unwanted columns
# df.drop([
#         'author_flair_css_class', 
#         'stickied', 
#         'retrieved_on', 
#         'author_flair_text',
#         'distinguished'
#         ], axis=1, inplace=True)

# Drop deleted users
# df = df[df.author != '[deleted]']

# parent_type t1=comment, t3=post
# df[['parent_type','parent_id']] = df.parent_id.str.split('_', expand=True)

# add parent author to row (if any) and drop rows without parents
# parent_map = df.set_index('id')['author']
# df['parent_author'] = df['parent_id'].map(parent_map)
# df.dropna(inplace=True, subset=['parent_author'])

#df.to_csv('reddit-2016-full.csv', encoding='utf-8')

df = pd.read_csv('reddit-2016-full.csv')


for num_comments in range(0,16):
    print(f'num_comments > {num_comments}')
    graph_data = get_edge_weights(df)

    graph_data = graph_data[graph_data['weight'] > num_comments]
    G = nx.from_pandas_edgelist(graph_data, 'author', 'parent_author', 'weight', create_using=nx.DiGraph())


    subgraphs = sorted(nx.strongly_connected_components(G), key=len, reverse=True)
    #print(f'largest subgraph: {len(subgraphs[0])}')
    #print(f'smallest subgraph: {len(subgraphs[-1])}')
    get_shortest_paths(subgraphs[0])
    print('----')

