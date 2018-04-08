#!/usr/bin/env python3

import argparse

import collections
import glob
import marshal
import pickle
import shutil
from functools import reduce
from itertools import chain

from air18.index.common import create_token_stream, create_index
from air18.index.spimi import save_spimi_blocks, merge_spimi_blocks
from air18.util.paths import *
from air18.index.map_reduce import segment_key, segment_keys, SegmentFile, air_map, \
    air_reduce
from air18.index.statistics import CollectionStatistics


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


def simple(files, params):
    token_stream, doc_stats, docid_docno_mapping, collection_statistics = create_token_stream(files, params)
    index = create_index(token_stream)

    with open(SIMPLE_INDEX_PATH, mode="wb") as index_file:
        marshal.dump(index, index_file)

    return doc_stats, docid_docno_mapping, collection_statistics


def spimi(files, params):
    token_stream, doc_stats, docid_docno_mapping, collection_statistics = create_token_stream(files, params)
    num_blocks = save_spimi_blocks(token_stream)
    print("Saved {} intermediate SPIMI blocks. Now merging".format(num_blocks))
    merge_spimi_blocks(num_blocks)
    return doc_stats, docid_docno_mapping, collection_statistics


def map_reduce(files, params):
    segments = collections.defaultdict(list)
    document_lengths_segment = dict()
    collection_statistics_segment = list()
    for token_stream, document_lengths, collection_statistics in (
    air_map(file, params) for file in files):

        # shuffle map results to segments
        for doc_token in token_stream:
            segments[segment_key(doc_token)].append(doc_token)


        document_lengths_segment.update(document_lengths)
        collection_statistics_segment.append(collection_statistics)

    # document_lengths and collection_statistics merge could be included in same air_reduce function as separate case
    # but was not for simplicity reason
    segment_indexes = {seg_key: air_reduce(segment) for seg_key, segment in segments.items()}
    collection_statistics = reduce(CollectionStatistics.merge, collection_statistics_segment)

    # save indexes, simple strategy: using pickle
    for seg_key in segment_keys:
        with SegmentFile(seg_key, mode="wb") as segment_file:
            pickle.dump(segment_indexes.get(seg_key), segment_file)

    return document_lengths_segment, collection_statistics


def main():
    params = parse_args()

    # get all files in specified directories recursively
    files = chain.from_iterable(
        (sorted(filter(os.path.isfile, glob.iglob('{}/**/*'.format(pattern), recursive=True))) for pattern in params.patterns))

    print("Clearing old index files")
    shutil.rmtree(INDEX_BASE, ignore_errors=True)
    os.makedirs(INDEX_BASE, exist_ok=True)

    print("Starting to index")
    if params.indexing_method == "simple":
        doc_stats, docid_docno_mapping, statistics = simple(files, params)
    elif params.indexing_method == "spimi":
        doc_stats, docid_docno_mapping, statistics = spimi(files, params)
    elif params.indexing_method == "map_reduce":
        doc_stats, statistics = map_reduce(files, params)
        docid_docno_mapping = None
    else:
        raise ValueError("Indexing method {} is unknown".format(params.indexing_method))

    # --------------- Save auxiliary index files --------------
    print("Saving statistics and settings to index directory")

    # save docid -> docno mapping
    with open(DOCID_DOCNO_MAPPING, "wb") as mapping_file:
        marshal.dump(docid_docno_mapping, mapping_file)

    # save document statistics
    with open(DOCUMENT_STATISTICS_FILEPATH, "wb") as stat_file:
        marshal.dump(doc_stats, stat_file)

    # save collection statistics
    with open(STATISTICS_FILEPATH, "wb") as stat_file:
        pickle.dump(statistics, stat_file)

    # save params so that the search script knows how to process query tokens
    with open(SETTINGS_FILEPATH, "wb") as settings_file:
        pickle.dump(params, settings_file)


if __name__ == '__main__':
    main()
