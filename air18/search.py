#!/usr/bin/env python3

import argparse
import pickle

from air18.segments import segment_keys


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("topic", type=argparse.FileType,
                        help="the topic file containing the query")

    subparsers = parser.add_subparsers(dest="similarity_function", title="similarity function")
    subparsers.required = True

    subparsers.add_parser("tf-idf", help="use TF-IDF")

    bm25_parser = subparsers.add_parser("bm25", help="use BM25")
    bm25_parser.add_argument("b", help="b parameter")
    bm25_parser.add_argument("k1", help="k1 parameter")

    bm25va_parser = subparsers.add_parser("bm25va", help="use BM25 Verboseness Fission Variant")
    bm25va_parser.add_argument("k1", help="k1 parameter")

    return parser.parse_args()


def main():
    params = parse_args()
    # TODO

    # load indexes
    segment_indexes = {}
    for seg_key in segment_keys:
        segment_indexes[seg_key] = pickle.load(open("index_" + str(seg_key) + ".p", "rb"))

    # print indexes
    for _, segment_index in segment_indexes.items():
        if segment_index:
            for token, docs in segment_index.items():
                print("token: " + token)
                for doc in docs:
                    print(doc)


if __name__ == '__main__':
    main()
