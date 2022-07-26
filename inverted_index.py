#!/usr/bin/env python

import argparse
import json
import os
from pyspark import SparkContext

def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce inverted index (Problem 1)')
    parser.add_argument('-d', help='path to data file', default='../data/mapreduce/books_small.json')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default='output')
    return parser.parse_args()

# Feel free to create more mappers and reducers.
def mapper1(record):
    #TODO
    for i in record[1].split():
        yield ((i, record[0]), 1)

def reducer1(a, b):
    # TODO
    return 1

def mapper2(record):
    # TODO
    yield (record[0][0], [record[0][1]])

def reducer2(a, b):
    # TODO
    return a + b


# Do not modify
def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d, 'r') as infile:
        data = [json.loads(line) for line in infile]
        #print(data)



    # TODO: build your pipeline
    inverted_index_result = sc.parallelize(data, 128).flatMap(mapper1).reduceByKey(reducer1)\
                                .flatMap(mapper2).reduceByKey(reducer2).collect()


    sc.stop()
    """
    output_inverted_index.json should be of the format -
    [
        [
            "dust",
            [
                "melville-moby_dick.txt"
            ]
        ],
        [
            "very",
            [
                "austen-emma.txt",
                "chesterton-thursday.txt",
                "edgeworth-parents.txt"
            ]
        ]
    ]

    """

    if not os.path.exists(args.o):
        os.makedirs(args.o)

    with open(args.o + '/output_inverted_index.json', 'w') as outfile:
        json.dump(inverted_index_result, outfile, indent=4)



if __name__ == '__main__':
    main()
