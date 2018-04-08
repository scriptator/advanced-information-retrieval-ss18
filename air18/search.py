#!/usr/bin/env python3

import argparse
import marshal
import operator
import pickle

import itertools
import sys
from collections import defaultdict
from math import log

from air18 import score
from air18.segments import segment_keys, SegmentFile
from air18.parsing import parse_topics
from air18.paths import *
from air18.blocks import from_block_line


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--topics-file", "-t", type=argparse.FileType(),
                        help="the topic file containing the query",
                        required=False,
                        default=DEFAULT_TOPIC_FILE)
    parser.add_argument("--show", type=int, default=1000,
                        help="Maximum number of documents to output per topic")
    parser.add_argument("--run-name", default="DefaultRun")
    parser.add_argument("--topic", default=None, help="Query only for one given topic instead of all")
    parser.add_argument("--debug", "-d", action="store_true", help="Print debug output")

    subparsers = parser.add_subparsers(dest="similarity_function", title="similarity function")
    subparsers.required = True

    tf_idf_parser = subparsers.add_parser("tf-idf", help="use TF-IDF")
    tf_idf_parser.set_defaults(scoring_function=score.tf_idf)

    bm25_parser = subparsers.add_parser("bm25", help="use BM25")
    bm25_parser.add_argument("--b", type=float, default=0.25, help="b parameter")
    bm25_parser.add_argument("--k1", type=float, default=1.5, help="k1 parameter")
    bm25_parser.set_defaults(scoring_function=score.bm25)

    bm25va_parser = subparsers.add_parser("bm25va", help="use BM25 Verboseness Fission Variant")
    bm25va_parser.add_argument("--k1", type=float, default=1.5, help="k1 parameter")
    bm25va_parser.set_defaults(scoring_function=score.bm25_verboseness_fission)

    return parser.parse_args()


def print_output(topic_id, sorted_scores, run_name, max_docs_per_topic):
    for rank, (docid, score) in enumerate(itertools.islice(sorted_scores, max_docs_per_topic)):
        print("{topic_id} Q0 {document_id} {rank} {score} {run_name}".format(
            topic_id=topic_id, document_id=docid, rank=rank, score=score,
            run_name=run_name
        ))


def main():
    params = parse_args()
    scoring_function = params.scoring_function
    max_docs_per_topic = params.show
    run_name = params.run_name

    # load index params to figure out how to process query tokens
    if params.debug:
        print("Loading index")
    settings_file_path = SETTINGS_FILEPATH
    if not os.path.isfile(settings_file_path):
        raise FileNotFoundError("Indexing settings file not found. Make sure "
                                "that indexing has finished successfully before you start a search.")
    with open(settings_file_path, "rb") as settings_file:
        index_params = pickle.load(settings_file)
    topics = parse_topics(params.topics_file, index_params.case_folding,
                          index_params.stop_words, index_params.stemming,
                          index_params.lemmatization)
    if params.topic is not None:
        try:
            topics = {params.topic: topics[params.topic]}
        except KeyError:
            print("ERROR: Requested topic {} does not exist in given topic file".format(params.topic), file=sys.stderr)
            exit(1)

    all_search_terms = set(itertools.chain.from_iterable(topics.values()))

    # load collection statistics
    with open(STATISTICS_FILEPATH, "rb") as stat_file:
        collection_statistics = pickle.load(stat_file)
        collection_statistics.finalize()    # precompute values

    # load document lengths
    with open(DOCUMENT_STATISTICS_FILEPATH, "rb") as norm_file:
        doc_stats = marshal.load(norm_file)

    # load indexes and keep only the relevant parts in memory
    if index_params.indexing_method == "map_reduce":
        index = {}
        for seg_key in segment_keys:
            with SegmentFile(seg_key) as segment_file:
                segment = pickle.load(segment_file)
                segment = segment if segment is not None else {}
                filtered_segment = {term: postings for term, postings in segment.items() if term in all_search_terms}
                index.update(filtered_segment)

    elif index_params.indexing_method == "simple":
        with open(SIMPLE_INDEX_PATH, "rb") as index_file:
            index = marshal.load(index_file)

    elif index_params.indexing_method == "spimi":
        index = {}
        with open(SPIMI_INDEX_PATH, "r") as index_file:
            with open(SPIMI_INDEX_INDEX_PATH, "rb") as meta_index_file:
                meta_index = marshal.load(meta_index_file)

                def find_term_postings(term):
                    line = index_file.readline()
                    if not line[:len(term)] == term:
                        raise RuntimeError("Meta-Index for '{}' points to wrong term '{}'".format(term, line[:len(term)]))
                    return from_block_line(line)[1]

                # iterate over sorted query terms to get the ideal disk access pattern
                for term in sorted(all_search_terms):
                    file_blockpos = meta_index[term]
                    index_file.seek(file_blockpos)
                    postings = find_term_postings(term)
                    if postings is not None:
                        index[term] = postings

    else:
        raise ValueError("Encountered unsupported index type {}".format(index_params.indexing_method))

    # final score per document is sum of scores s_t,f occurring in query and document
    if params.debug:
        print("Starting to score documents")
    b = getattr(params, "b", None)
    k1 = getattr(params, "k1", None)
    for topic_num, terms in topics.items():
        scores = defaultdict(float)
        for search_term in terms:
            # ignore term if it is not contained in any document
            if search_term in index:
                postings = index[search_term]
                df_t = len(postings)
                idf_t = log(collection_statistics.num_documents / df_t)
                for docid, tf in postings:
                    doc_length, doc_avgtf = doc_stats[docid]
                    doc_score = scoring_function(tf_td=tf, idf_t=idf_t, dl=doc_length, avgtf=doc_avgtf,
                                             collection_statistics=collection_statistics,
                                             b=b, k1=k1)
                    scores[docid] += doc_score
        scores = sorted(scores.items(), key=operator.itemgetter(1), reverse=True)
        print_output(topic_num, scores, run_name, max_docs_per_topic)


if __name__ == '__main__':
    main()
