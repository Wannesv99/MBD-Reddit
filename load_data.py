import subprocess
import glob
import pandas as pd
import json
import networkx as nx

# Requires zstd package to be installed
def decompress_zst(in_path, out_path):
    for f in glob.glob(f'{in_path}/*.zst'):
        # name format: reddit-yyyy-nn
        output = out_path + '/reddit_' + f.split('_')[1].split('.')[0]
        
        # decompress
        print(f'Decompressing: {f}')
        subprocess.run(['unzstd', f, '--memory=2048MB', '-o', output])


files = glob.glob('./data/2016/reddit*')

frames = []
for file in files:
    # Read data line by line
    data = []
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))

    # Convert to dataframe
    frame = pd.DataFrame(data)
    frames.append(frame)

df = pd.concat(frames, ignore_index=True)


# Drop some unwanted columns
df.drop([
        'author_flair_css_class', 
        'stickied', 
        'retrieved_on', 
        'author_flair_text'
        ], axis=1, inplace=True)

# Drop deleted users
df = df[df.author != '[deleted]']

# parent_type t1=comment, t3=post
df[['parent_type','parent_id']] = df.parent_id.str.split('_', expand=True)

# add parent author to row (if any) and drop rows without parents
parent_map = df.set_index('id')['author']
df['parent_author'] = df['parent_id'].map(parent_map)
df.dropna(inplace=True, subset=['parent_author'])


print(len(df))

# Create network graph
G = nx.from_pandas_edgelist(df, 'author', 'parent_author')

