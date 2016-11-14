#!usr/bin/env python

import argparse
import collections

nodes = {}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('batch', help='batch payment file', default='batch_payments.csv')
    parser.add_argument('stream', help='stream payment file', default='stream_payments.csv')
    parser.add_argument('out1', help='out1', default='output1.txt')
    parser.add_argument('out2', help='out2', default='output2.txt')
    parser.add_argument('out3', help='out3', default='output3.txt')
    args = parser.parse_args()

    populate_graph(args.batch)
    feature1(args.stream, args.out1)
    feature2(args.stream, args.out2)
    feature3(args.stream, args.out3)

def populate_graph(file):
    #parse the raw data to parepare for graph path finding
    fd = open(file, 'r')
    fd.readline()
    line = fd.readline()
    print 'Loading batch file.'
    while line:
        columns = line.split(',')
        node1 = columns[1].lstrip()
        node2 = columns[2].lstrip()
        create_connection(node1, node2)
        line = fd.readline()


def create_connection(user1, user2):
    #set up nodes/vertex connections
    if user1 not in nodes:
        nodes[user1] = set()
    if user2 not in nodes:
        nodes[user2] = set()
    nodes[user1].add(user2)
    nodes[user2].add(user1)

def bfs(nodes,start,end):
    #bfs should be the most efficient methond after considering a star, dfs, adjacency list
    #each layer of user connection is counted to allow 4th layer friends' transactions to be valid
    layer = 0
    visited = set()
    res = None
    d = collections.deque()
    d.append(start)
    while d and layer<5:
        new = d.popleft()
        if new not in visited and new in nodes:
            layer += 1
            visited.add(new)
            if end in nodes[new]:
                res =  True
            else:
                d.extend(nodes[new] - visited)
                res = False
        else: res = False
    return res


#=================================================================================


def feature1(file, output_file):
    with open(file, 'r') as fh, open(output_file,'w') as f:
        fh.readline()
        line = fh.readline()
        while line:
            columns = line.split(',')
            node3 = columns[1].lstrip()
            node4 = columns[2].lstrip()
            line = fh.readline()
            if node3 in nodes:
                if node4 in nodes:
                    f.write ('trusted\n')
            else:
                f.write ('unverified\n')

def feature2(file, output_file):
    with open(file, 'r') as fh, open(output_file,'w') as f:
        fh.readline()
        line = fh.readline()
        while line:
            columns = line.split(',')
            node3 = columns[1].lstrip()
            node4 = columns[2].lstrip()
            line = fh.readline()
            if node3 in nodes:
                if node4 in nodes[node3] or node4 in nodes:
                    f.write ('trusted\n')
            else:
                f.write ('unverified\n')

def feature3(file, output_file):
    with open(file, 'r') as fh, open(output_file,'w') as f:
        fh.readline()
        line = fh.readline()
        while line:
            columns = line.split(',')
            node5 = columns[1].lstrip()
            node6 = columns[2].lstrip()
            line = fh.readline()
            if node5 in nodes and node6 in nodes[node5]:
                f.write ('trusted\n')
            else:
                res = file.bfs(nodes,node5,node6)
                if res:
                    f.write ('trusted\n')
                else:
                    f.write ('unverified\n')

if __name__ == "__main__":
    main()