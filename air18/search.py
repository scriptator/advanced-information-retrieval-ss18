#!/usr/bin/env python3

import argparse
import pickle

import os

from air18.segments import segment_keys, SegmentFile
from air18.topics import parse_topics


DEFAULT_TOPIC_FILE=os.path.join(os.path.dirname(__file__), "topicsTREC8Adhoc.txt")


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--topics-file", "-t", type=argparse.FileType(),
                        help="the topic file containing the query",
                        required=False,
                        default=DEFAULT_TOPIC_FILE)

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

    # load index params to figure out how to process query tokens
    settings_file_path = "../indexed_data/settings.p"
    if not os.path.isfile(settings_file_path):
        raise FileNotFoundError("Indexing settings file not found. Make sure"
                                "that indexing has finished successfully before you start a search.")
    with open(settings_file_path, "rb") as settings_file:
        index_params = pickle.load(settings_file)
    topics = parse_topics(params.topics_file, index_params.case_folding,
                          index_params.stop_words, index_params.stemming,
                          index_params.lemmatization)
    # TODO

    # load indexes
    segment_indexes = {}
    for seg_key in segment_keys:
        with SegmentFile(seg_key) as segment_file:
            segment_indexes[seg_key] = pickle.load(segment_file)

    # print indexes
    for _, segment_index in segment_indexes.items():
        if segment_index:
            for token, docs in segment_index.items():
                print("token: " + token)
                for doc in docs:
                    print(doc)


if __name__ == '__main__':
    main()
