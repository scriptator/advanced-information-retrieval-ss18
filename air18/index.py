#!/usr/bin/env python3

import argparse
import operator
from collections import Iterable
from typing import Tuple, Dict, Union

import collections
import glob
import heapq
import marshal
import pickle
import shutil
from functools import partial, reduce
from itertools import chain, groupby

import more_itertools

from air18.blocks import BlockFile, BLOCK_SIZE, block_line, from_block_line
from air18.parsing import parse_json, parse_xml
from air18.paths import *
from air18.progress import ProgressBar
from air18.segments import segment_key, segment_keys, SegmentFile
from air18.statistics import CollectionStatistics
from air18.tokens import air_tokenize


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


def parse_and_process_file(file, params, docid_docno_mapping: Union[Dict[int, str], None],
                           doc_stats: Dict[int, Tuple],
                           collection_statistics: CollectionStatistics):
    """
    Parse and tokenize file.

    :param file: path to the input file
    :param params: argparse params
    :param docid_docno_mapping: a dictionary containing the mapping, or None if mapping should be disabled
    :param doc_stats: a dictionary filled with statistics per document
    :param collection_statistics:
    :return: nothing, yield Tuples (docid, token)
    """
    if docid_docno_mapping is None:
        map_docid = False
    else:
        map_docid = True

    print("Parsing file {}".format(file))
    with open(file, encoding="iso-8859-1") as f:
        if file.endswith(".json"):
            data = parse_json(f)
        else:
            data = parse_xml(f)

        for docno, text in data:
            if map_docid:
                docid = collection_statistics.num_documents
                docid_docno_mapping[docid] = docno
            else:
                docid = docno

            dl = 0
            unique_terms = set()
            for token in air_tokenize(text, params.case_folding, params.stop_words,
                                      params.stemming, params.lemmatization):
                dl += 1
                unique_terms.add(token)
                yield (docid, token)

            if len(unique_terms) > 0:
                # save document statistics
                avgtf = dl / len(unique_terms)
                doc_stats[docid] = (dl, avgtf)

                # update collection statistics
                collection_statistics.total_doc_length += dl
                collection_statistics.sum_avgtf += avgtf
                collection_statistics.num_documents += 1


def create_token_stream(files, params):
    doc_stats = {}
    docid_docno_mapping = {}
    statistics = CollectionStatistics()
    parse_fn = partial(parse_and_process_file, params=params,
                       docid_docno_mapping=docid_docno_mapping,
                       doc_stats=doc_stats,
                       collection_statistics=statistics)
    token_stream = chain.from_iterable(parse_fn(file=file) for file in files)
    return token_stream, doc_stats, docid_docno_mapping, statistics


def create_index(doc_tokens: Tuple[Union[str, int], str]):
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


def air_map(file, params):
    doc_stats = {}
    collection_statistics = CollectionStatistics()
    token_stream = parse_and_process_file(file, params, docid_docno_mapping=None,
                                          doc_stats=doc_stats,
                                          collection_statistics=collection_statistics)
    return token_stream, doc_stats, collection_statistics


def air_reduce(segment):
    return create_index(segment)


def simple(files, params):
    token_stream, doc_stats, docid_docno_mapping, collection_statistics = create_token_stream(files, params)
    index = create_index(token_stream)

    with open(SIMPLE_INDEX_PATH, mode="wb") as index_file:
        marshal.dump(index, index_file)

    return doc_stats, docid_docno_mapping, collection_statistics


def save_spimi_blocks(doc_tokens):
    blocks = more_itertools.chunked(doc_tokens, BLOCK_SIZE)
    block_indexes = (sorted(create_index(block).items(), key=operator.itemgetter(0)) for block in blocks)

    num_blocks = 0
    num_terms = 0
    for blockno, block_index in enumerate(block_indexes, 1):
        with BlockFile(blockno, mode="w") as index_file:
            for token, docid_tfs in block_index:
                index_file.write(block_line(token, docid_tfs))
        num_blocks = blockno
        num_terms += len(block_index)

    return num_blocks, num_terms


def merge_spimi_blocks(num_blocks, num_terms):
    def merge_postings(p1, p2):
        # Merges sorted inputs into a single sorted output.
        # a posting is a tuple of docid, tf_td
        merged_postings_with_dup = heapq.merge(p1[1], p2[1], key=operator.itemgetter(0))
        merged_postings = [(docid, sum((tf for _, tf in docid_tfs)))
                           for docid, docid_tfs in groupby(merged_postings_with_dup, key=operator.itemgetter(0))]
        return p1[0], merged_postings

    block_files = {blockno: BlockFile(blockno, mode="r").open() for blockno in range(1, num_blocks + 1)}

    # load first line from all blocks
    blocks = dict()
    for no, file in block_files.items():
        blocks[no] = from_block_line(file.readline())

    meta_index = {}
    progressbar = ProgressBar("Merging index blocks", num_terms)
    with open(SPIMI_INDEX_PATH, mode="w") as index_file:
        while blocks:
            progressbar.next()

            # get posting lists of smallest token from blocks, merge them if more than one posting list
            min_token = min((posting[0] for posting in blocks.values()))
            min_token_blocknos = (no for no, posting in blocks.items() if posting[0] == min_token)
            min_token_postings = (posting for posting in blocks.values() if posting[0] == min_token)
            merged_posting = reduce(merge_postings, min_token_postings)

            # add index file offset for term to meta index
            meta_index[min_token] = index_file.tell()

            # save merged postings list to index file
            index_file.write(block_line(*merged_posting))

            # reload line from all written out blocks and remove EOF blocks from dictionary
            for no in min_token_blocknos:
                blocks[no] = None

            for no, file in block_files.items():
                if no in blocks and blocks[no] is None:
                    blocks[no] = from_block_line(file.readline())
                    if blocks[no] is None:
                        del blocks[no]

    progressbar.finish()

    # save meta index
    with open(SPIMI_INDEX_INDEX_PATH, mode="wb") as index_index_file:
        marshal.dump(meta_index, index_index_file)

    # remove intermediate SPIMI files
    for block_file in block_files.values():
        block_file.close()
        os.remove(block_file.name)


def spimi(files, params):
    token_stream, doc_stats, docid_docno_mapping, collection_statistics = create_token_stream(files, params)
    num_blocks, num_terms = save_spimi_blocks(token_stream)
    print("Saved {} intermediate SPIMI blocks. Now merging".format(num_blocks))
    merge_spimi_blocks(num_blocks, num_terms)
    return doc_stats, docid_docno_mapping, collection_statistics


def map_reduce(files, params):
    segments = collections.defaultdict(list)
    document_lengths_segment = dict()
    collection_statistics_segment = list()
    for token_stream, document_lengths, collection_statistics in (air_map(file, params) for file in files):

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
    files = sorted(chain.from_iterable(
        glob.iglob('{}/**/*'.format(pattern), recursive=True)
        for pattern in params.patterns))

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
