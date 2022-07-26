#!/usr/bin/env python

from __future__ import division
import argparse
import json
import math
from os.path import dirname, realpath
from pyspark import SparkContext
import time
import os

VIRTUAL_COUNT = 10
PRIOR_CORRELATION = 0.0
THRESHOLD = 0.5


##### Metric Functions ############################################################################
def correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy):
    # http://en.wikipedia.org/wiki/Correlation_and_dependence
    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt(n * sum_xx - sum_x * sum_x) * math.sqrt(n * sum_yy - sum_y * sum_y)
    if denominator == 0:
        return 0.0
    return numerator / denominator

def regularized_correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy, virtual_count, prior_correlation):
    unregularized_correlation_value = correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy)
    weight = n / (n + virtual_count)
    return weight * unregularized_correlation_value + (1 - weight) * prior_correlation

def cosine_similarity(sum_xx, sum_yy, sum_xy):
    # http://en.wikipedia.org/wiki/Cosine_similarity
    numerator = sum_xy
    denominator = (math.sqrt(sum_xx) * math.sqrt(sum_yy))
    if denominator == 0:
        return 0.0
    return numerator / denominator

def jaccard_similarity(n_common, n1, n2):
    # http://en.wikipedia.org/wiki/Jaccard_index
    numerator = n_common
    denominator = n1 + n2 - n_common
    if denominator == 0:
        return 0.0
    return numerator / denominator
#####################################################################################################

##### util ##########################################################################################
def combinations(iterable, r):
    # http://docs.python.org/2/library/itertools.html#itertools.combinations
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(list(range(r))):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)
#####################################################################################################


def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce similarities')
    parser.add_argument('-d', help='path to data directory', default='../data/mapreduce/recommendations/small/')
    parser.add_argument('-u', help='your gcp username, only set this when you submit a job to a cluster', default='')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default="output")
    return parser.parse_args()

# Feel free to create more mappers and reducers.

#record: string of the form
#        user_id::movie_id::rating
# or     movie_id::movie_title
#output: (movie_id, [[movie_title]])
# or     (movie_id, [[user_id, rating]])
def mapper0(record):
    # TODO
    row = record.split('::')
    if len(row) > 2:
        return (row[1], [[row[0], int(row[2])]])

    else:
        return (row[0], [[row[1]]])

#a: a list of lists, [[a_item1, a_item2, ...]]
#b: a list of lists, [[b_item1, b_item2, ...]]
#output: a list of lists, [[a_item1, a_item2, ...], [b_item1, b_item2, ...]]
def reducer(a, b):
    # TODO
    return a + b

#record: (movie_id, [[movie_title], [user1_id, rating], [user1_id, rating], ...])
#                   list items can be in any order
#output: (movie_title, [[user_id, rating]])
def mapper1(record):
    # Hint:
    # INPUT:
    #   record: (key, values)
    #     where -
    #       key: movie_id
    #       values: a list of values in the line
    # OUTPUT:
    #   [(key, value), (key, value), ...]
    #     where -
    #       key: movie_title
    #       value: [(user_id, rating)]
    #
    # TODO
    for i in record[1]:
        if len(i) == 1:
            movie_title = i[0]
            break
    for i in record[1]:
        if len(i) == 2:
            yield (movie_title, [i])

#record: (movie_title, [[user1_id, rating], [user2_id, rating], ...])
#output: (user_id, [[movie_title, rating, num_ratings_of_movie]])
def mapper2(record):
    # TODO
    n = len(record[1])
    for i in record[1]:
        yield (i[0], [[record[0], i[1], n]])

#record: (user_id, [[movie1, rating, num_ratings_of_movie1],
#                   [movie2, rating num_ratings_of_movie2], ...])
#output: ((movie_a, movie_b), [1, rating_a, rating_a ^ 2, rating_b, rating_b ^ 2,
#                              rating_a * rating_b. num_ratings_of_movie1,
#                              num_ratings_of_movie2])
def mapper3(record):
    # TODO
    for i in combinations(record[1], 2):
        if i[0][0] < i[1][0]:
            movie1 = i[0]
            movie2 = i[1]
        else:
            movie1 = i[1]
            movie2 = i[0]

        yield ((movie1[0], movie2[0]), [1, movie1[1], movie1[1]**2, movie2[1],
        movie2[1]**2, movie1[1] * movie2[1], movie1[2], movie2[2]])

#a = [count_a, rating_a1, a_rating_a1 ^ 2, rating_b1, rating_b1 ^ 2, rating_a1 * rating_b1,
#     num_ratings_of_movie1, num_ratings_of_movie2])
#b = [count_b, rating_a2, rating_a2 ^ 2, rating_b2, rating_b2 ^ 2, rating_a2 * rating_b2,
#     num_ratings_of_movie1, num_ratings_of_movie2])
# count = num_users_who_rated_both
#output: [count_a + count_b, rating_a1 + rating_a2, rating_a1 ^ 2 + rating_a2 ^ 2,
#         rating_b1 + rating_b2, rating_b1 ^ 2 + rating_b2 ^ 2,
#         rating_a1 * rating_b1 + rating_a2 * rating_b2, num_ratings_of_movie1,
#         num_ratings_of_movie2]

def reducer2(a, b):
    return [a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], a[4] + b[4], a[5] + b[5], a[6], a[7]]

#record: ((movie1, movie2), [n, sum_ratings_x, sum_ratings_x_squared,
#                            sum_ratings_y, sum_ratings_y_squared
#                            sum_ratings_xy, n1, n2])
#output: [(movie1, [[movie2, correlation, regularized_correlation,
#                    cosine_similarity, jaccard_similarity, n, n1, n2]])]
# or     [] if threshold not met
def mapper4(record):
    n = record[1][0]
    sum_x = record[1][1]
    sum_xx = record[1][2]
    sum_y = record[1][3]
    sum_yy = record[1][4]
    sum_xy = record[1][5]
    n1 = record[1][6]
    n2 = record[1][7]
    cor = correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy)
    reg_cor = regularized_correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy, VIRTUAL_COUNT, PRIOR_CORRELATION)
    cos = cosine_similarity(sum_xx, sum_yy, sum_xy)
    jacc = jaccard_similarity(n, n1, n2)

    if reg_cor > THRESHOLD:
        #print(record[0][0], [[record[0][1], cor, reg_cor, cos, jacc, n, n1, n2]])
        return [(record[0][0], [[record[0][1], cor, reg_cor, cos, jacc, n, n1, n2]])]
    else:
        return []


#record: (movie1, [[movie2, list_of_metrics], [movie3, list_of_metrics], ...])
#output: ((movie1, movie_n), list_of_metrics)
def mapper5(record):
    # TODO
    for i in record[1]:
        yield ((record[0], i[0]), i[1:])

def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d + '/movies.dat', 'r') as mlines:
        data = [line.rstrip() for line in mlines]
    with open(args.d + '/ratings.dat', 'r') as rlines:
        data += [line.rstrip() for line in rlines]

    # Implement your mapper and reducer function according to the following query.
    # stage1_result represents the data after it has been processed at the second
    # step of map reduce, which is after mapper1.
    stage1_result = sc.parallelize(data, args.n).map(mapper0).reduceByKey(reducer) \
                                                    .flatMap(mapper1).reduceByKey(reducer)

    #print(stage1_result.collect())
    if not os.path.exists(args.o):
        os.makedirs(args.o)

    # Store the stage1_output
    with open(args.o  + '/netflix_stage1_output.json', 'w') as outfile:
        json.dump(stage1_result.collect(), outfile, separators=(',', ':'))

    # TODO: continue to build the pipeline
    # Pay attention to the required format of stage2_result
    stage2_result = stage1_result.flatMap(mapper2).reduceByKey(reducer).flatMap(mapper3)\
                                                .reduceByKey(reducer2).flatMap(mapper4).reduceByKey(reducer)

    # Store the stage2_output
    with open(args.o  + '/netflix_stage2_output.json', 'w') as outfile:
        json.dump(stage2_result.collect(), outfile, separators=(',', ':'))

    # TODO: continue to build the pipeline
    final_result = stage2_result.flatMap(mapper5).collect()


    with open(args.o + '/netflix_final_output.json', 'w') as outfile:
        json.dump(final_result, outfile, separators=(',', ':'))

    sc.stop()

    # Don't modify the following code
    if args.u != '':
        os.system("gsutil mv {0} gs://{1}".format(args.o + '/*.json', args.u))


if __name__ == '__main__':
    main()
