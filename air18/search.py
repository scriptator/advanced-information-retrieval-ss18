#!/usr/bin/env python3

import argparse
import pickle

import os

import itertools
from collections import defaultdict

from air18 import score
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

    tf_idf_parser = subparsers.add_parser("tf-idf", help="use TF-IDF")
    tf_idf_parser.set_defaults(scoring_function=score.tf_idf)

    bm25_parser = subparsers.add_parser("bm25", help="use BM25")
    bm25_parser.add_argument("b", help="b parameter")
    bm25_parser.add_argument("k1", help="k1 parameter")
    bm25_parser.set_defaults(scoring_function=score.bm25)

    bm25va_parser = subparsers.add_parser("bm25va", help="use BM25 Verboseness Fission Variant")
    bm25va_parser.add_argument("k1", help="k1 parameter")
    bm25va_parser.set_defaults(scoring_function=score.bm25_verboseness_fission)

    return parser.parse_args()


def main():
    params = parse_args()
    scoring_function = params.scoring_function

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
    all_search_terms = set(itertools.chain.from_iterable(topics.values()))

    # load collection statistics
    with open("../indexed_data/statistics.p", "rb") as stat_file:
        collection_statistics = pickle.load(stat_file)

    # load indexes and keep only the relevant parts in memory
    index = {}
    for seg_key in segment_keys:
        with SegmentFile(seg_key) as segment_file:
            segment = pickle.load(segment_file)
            segment = segment if segment is not None else {}
            filtered_segment = {term: postings for term, postings in segment.items() if term in all_search_terms}
            index.update(filtered_segment)

    # final score per document is sum of scores s_t,f occurring in query and document
    scores = defaultdict(float)
    for search_term in all_search_terms:
        # ignore term if it is not contained in any document
        if search_term in index:
            postings = index[search_term]
            df_t = len(postings)
            for docid, tf in postings:
                score = scoring_function(tf_td=tf, df_t=df_t,
                                         collection_statistics=collection_statistics)
                scores[docid] += score

    print(scores)


if __name__ == '__main__':
    main()
