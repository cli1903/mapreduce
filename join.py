#!/usr/bin/env python

import argparse
import json
import os
from os.path import dirname, realpath

from pyspark import SparkContext


def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce join (Problem 2)')
    parser.add_argument('-d', help='path to data file', default='../data/mapreduce/records.json')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default='output')
    return parser.parse_args()


# Feel free to create more mappers and reducers.
def mapper1(record):
    # TODO
    yield(record[2], [record])

def mapper2(record):
    yield(record[1][0][2], record[1])


def reducer1(a, b):
    # TODO
    '''
    print(type(a))
    print(type(b))
    print("--------------------\n")
    '''
    return a + b

def mapper2(record):
    # TODO

    for i in record[1]:
        for j in record[1]:
            row = i + j
            yield row


    #yield record

def filterer(record):
    return ((record[0] == 'rele') and (record[17] == 'disp'))



def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d, 'r') as infile:
        data = [json.loads(line) for line in infile]

    #print(data)

    # TODO: build your pipeline
    join_result = sc.parallelize(data, 128).flatMap(mapper1).reduceByKey(reducer1)\
                                        .flatMap(mapper2).filter(filterer).collect()

    sc.stop()

    if not os.path.exists(args.o):
        os.makedirs(args.o)

    with open(args.o + '/output_join.json', 'w') as outfile:
        json.dump(join_result, outfile, indent=4)


if __name__ == '__main__':
    main()
