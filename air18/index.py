#!/usr/bin/env python3

import argparse
import glob
import marshal
from functools import partial

import collections
import pickle
import itertools
import shutil

from air18.segments import segment_key, segment_keys, SegmentFile
from air18.statistics import CollectionStatistics
from air18.tokens import air_tokenize
from air18.util import parse_json, parse_xml
from air18.paths import *


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("patterns",
                        nargs="+",
                        help="the directory (recursively) containing files or a single file in XML format to index")
    parser.add_argument("--case-folding", action="store_true", help="apply case folding")
    parser.add_argument("--stop-words", action="store_true", help="remove stop words")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--stemming", action="store_true", help="apply stemming")
    group.add_argument("--lemmatization", action="store_true", help="apply lemmatization")

    parser.add_argument("--indexing-method",
                        choices=["simple", "spimi", "map_reduce"],
                        required=True,
                        help="Indexing method to use")

    return parser.parse_args()


def parse_and_process_file(file, params, document_lengths, collection_statistics: CollectionStatistics):
    print("Parsing file {}".format(file))
    with open(file, encoding="iso-8859-1") as f:
        if file.endswith(".json"):
            data = parse_json(f)
        else:
            data = parse_xml(f)

        for docno, text in data:
            dl = 0
            unique_terms = set()
            for token in air_tokenize(text, params.case_folding, params.stop_words,
                                      params.stemming, params.lemmatization):
                dl += 1
                unique_terms.add(token)
                yield (docno, token)

            if len(unique_terms) > 0:
                # save document statistics
                avgtf = dl / len(unique_terms)
                document_lengths[docno] = (dl, avgtf)

                # update collection statistics
                collection_statistics.total_doc_length += dl
                collection_statistics.sum_avgtf += avgtf
                collection_statistics.num_documents += 1


def create_index(doc_tokens):
    # simple index without major performance considerations
    index = collections.defaultdict(list)

    # invert step: add everything to a list
    for docid, token in doc_tokens:
        index[token].append(docid)

    # convert list to tuples (docid, tf)
    for token, postings in index.items():
        counter = collections.Counter(postings)
        index[token] = sorted(counter.items())

    # convert defaultdict to dict
    return dict(index)


def reduce(segment):
    return create_index(segment)


def simple(token_stream, params):
    index = create_index(token_stream)

    with open(SIMPLE_INDEX_PATH, mode="wb") as index_file:
        marshal.dump(index, index_file)


def spimi(token_stream, params):
    pass


def map_reduce(token_stream, params):
    segments = collections.defaultdict(list)
    for doc_token in token_stream:
        segments[segment_key(doc_token)].append(doc_token)

    segment_indexes = {seg_key : reduce(segment) for seg_key, segment in segments.items()}

    # save indexes, simple strategy: using pickle
    for seg_key in segment_keys:
        with SegmentFile(seg_key, mode="wb") as segment_file:
            pickle.dump(segment_indexes.get(seg_key), segment_file)


def main():
    params = parse_args()

    # get all files in specified directories recursively
    files = itertools.chain.from_iterable(
        glob.iglob('{}/**/*'.format(pattern), recursive=True)
        for pattern in params.patterns)

    document_lengths = {}
    statistics = CollectionStatistics()
    parse_fn = partial(parse_and_process_file, params=params, document_lengths=document_lengths,
                       collection_statistics=statistics)
    token_stream = itertools.chain.from_iterable(parse_fn(file=file) for file in files)

    shutil.rmtree(INDEX_BASE, ignore_errors=True)
    os.makedirs(INDEX_BASE, exist_ok=True)

    if params.indexing_method == "simple":
        simple(token_stream, params)
    if params.indexing_method == "spimi":
        spimi(token_stream, params)
    if params.indexing_method == "map_reduce":
        # TODO currently our map phase uses only one block, try to orient more on slide 57 and also calculate collection statistics during the map phase in parallel and merge afterwards
        map_reduce(files, params)

    # save document lengths
    with open(DOCUMENT_STATISTICS_FILEPATH, "wb") as norm_file:
        marshal.dump(document_lengths, norm_file)

    # save statistics
    with open(STATISTICS_FILEPATH, "wb") as stat_file:
        pickle.dump(statistics, stat_file)

    # save params so that the search script knows how to process query tokens
    with open(SETTINGS_FILEPATH, "wb") as settings_file:
        pickle.dump(params, settings_file)


if __name__ == '__main__':
    main()
