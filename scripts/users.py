import copy
import os

import pyvis
import pandas
import networkx
import json
from matplotlib import pyplot as plt
import scipy

counters = [0, 0, 0]
G = networkx.DiGraph()
df_submissions = pandas.read_json("data_json/RS_2006-11.json", lines=True)
df_submissions = df_submissions[df_submissions["num_comments"] > 1]
df_submissions = df_submissions[["author", "id"]]
df_comments = pandas.read_json("data_json/RC_2006-11.json", lines=True)
df_comments = df_comments[["id", "parent_id", "link_id", "author"]]
missed_users = set()


def init_values(month):
    month = month if month >= 10 else f"0{month}"
    counters = [0, 0, 0]
    G = networkx.DiGraph()
    df_submissions = pandas.read_json(f"data_json/RS_2006-{month}.json", lines=True)
    df_submissions = df_submissions[df_submissions["num_comments"] > 1]
    df_submissions = df_submissions[["author", "id"]]
    df_comments = pandas.read_json(f"data_json/RC_2006-{month}.json", lines=True)
    df_comments = df_comments[["id", "parent_id", "link_id", "author"]]
    missed_users = set()
    return "s"


def add_edge(comment):
    if comment['author'] == "[deleted]":
        return
    counters[2] += 1
    if counters[2] % 2000 == 0:
        print(counters)
    try:
        if comment["link_id"] == comment["parent_id"]:
            #print(comment)
            submission = df_submissions[df_submissions["id"] == comment["parent_id"].split("_")[1]]
            if len(submission) != 1:
                counters[1] += 1
            else:
                #print(comment)
                if submission["author"].iloc[0] == "[deleted]":
                    missed_users.add(comment["author"])
                    return
                G.add_edge(comment["author"], submission["author"].iloc[0])
                counters[0] += 1
        else:
            comment_replied_to = df_comments[df_comments["id"] == comment["parent_id"].split("_")[1]]
            if len(comment_replied_to) != 1:
                counters[1] += 1
            else:
                if comment_replied_to["author"].iloc[0] == "[deleted]":
                    missed_users.add(comment["author"])
                    return
                G.add_edge(comment["author"], comment_replied_to["author"].iloc[0])
                counters[0] += 1
    except KeyError:
        print(comment)
        raise KeyError


def shortest_paths_min_vert(min_vertices=0):
    G = networkx.read_gpickle("graph.pickle")
    outdeg = G.out_degree()
    to_remove = [n[0] for n in outdeg if n[1] < min_vertices]
    print(G.number_of_nodes())
    G.remove_nodes_from(to_remove)
    print(G.number_of_nodes())
    shortest_paths = networkx.all_pairs_shortest_path(G)
    shortest_paths_dict = dict(shortest_paths)
    max_path_length = 0
    max_path = {}
    path_lengths = {}
    for author1, paths in shortest_paths_dict.items():
        for author2, path in paths.items():
            if len(path) in path_lengths:
                path_lengths[len(path)] += 1
            else:
                path_lengths[len(path)] = 1
            if len(path) > max_path_length:
                max_path["author1"] = author1
                max_path["author2"] = author2
                max_path["path"] = path
                max_path_length = len(path)
    print(max_path)
    print(path_lengths)

def check_shortest_paths():
    for i in range(0, 50, 2):
        print(f"Shortest path for min {i} outgoing vertices")
        shortest_paths_min_vert(i)


def visualize():
    G = networkx.read_gpickle("graph.pickle")
    min_vertices = 40
    outdeg = G.out_degree()
    to_remove = [n[0] for n in outdeg if n[1] < min_vertices]
    G.remove_nodes_from(to_remove)
    net = pyvis.network.Network(notebook=False, directed=True)
    net.from_nx(G)
    net.show_buttons()
    net.toggle_physics(False)
    net.show("graph1.html")
    print(max(outdeg, key=lambda x: x[1]))


def files_to_pickle():
    json_path = os.path.join(os.getcwd(), "data_json")
    for file_s in os.listdir(json_path):
        if "RS" in file_s:
            for file_c in os.listdir(json_path):
                if "RC" in file_c and file_c[2:] == file_s[2:]:
                    print(file_c, file_s)
                    G = networkx.DiGraph()
                    df_submissions = pandas.read_json(os.path.join(json_path, file_s), lines=True)
                    df_comments = pandas.read_json(os.path.join(json_path, file_c), lines=True)
                    df_submissions = df_submissions[df_submissions["num_comments"] > 1]
                    df_submissions = df_submissions[["author", "id"]]
                    df_comments = df_comments[["id", "parent_id", "link_id", "author"]]
                    df_comments.apply(lambda x: add_edge(x), axis=1)
                    networkx.write_gpickle(G, os.path.join("./pickles", file_s[2:] + ".pickle"))

if __name__ == '__main__':
    # for i in range(1, 13):
    # init_values(i)
    # df_comments.apply(lambda x: add_edge(x), axis='columns')
    # networkx.write_gpickle(G, f"graph_{i}.pickle")
    # print(networkx.read_gpickle(f"graph_{i}.pickle"))
    visualize()


