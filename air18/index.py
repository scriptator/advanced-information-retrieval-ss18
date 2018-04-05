#!/usr/bin/env python3

import argparse
import glob
import json
import collections
import xml.etree.ElementTree as ET
import pickle
import itertools
import more_itertools
import os
import shutil
import functools

from air18.segments import segment_key, segment_keys, SegmentFile
from air18.statistics import CollectionStatistics
from air18.tokens import air_tokenize
from air18.blocks import BlockFile, blocksize, block_line, from_block_line


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


def parse_and_process_file(file, params):
    print("Parsing file {}".format(file))
    with open(file, encoding="iso-8859-1") as f:
            if file.endswith(".json"):
                data = parse_json(f)
            else:
                data = parse_xml(f)

            for docno, text in data:
                for token in air_tokenize(text, params.case_folding, params.stop_words,
                                          params.stemming, params.lemmatization):
                    yield (docno, token)


def parse_xml(file):
    root = ET.fromstringlist(["<ROOT>", file.read(), "</ROOT>"])
    for doc in root.findall("DOC"):
        docno = doc.find("DOCNO").text.strip()
        text = "\n".join(doc.find("TEXT").itertext())
        # Documents that do not have a <TEXT> tag can be ignored
        if text != "":
            yield docno, text


def parse_json(file):
    doclist = json.load(file)
    for doc in doclist:
        # Documents that do not have a <TEXT> tag can be ignored
        if doc["text"] is not None:
            yield doc["docno"], doc["text"]


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

    return index


def segmentize(file, params):
    segments = collections.defaultdict(list)

    for doc_token in parse_and_process_file(file, params):
        segments[segment_key(doc_token)].append(doc_token)

    return segments


def reduce(segment):
    return create_index(segment)


def compute_collection_statistics(segment_indexes):
    # TODO calculate properly
    return CollectionStatistics(num_documents=100000)


def simple(files, params):
    doc_tokens = itertools.chain.from_iterable(parse_and_process_file(file, params) for file in files)
    index = create_index(doc_tokens)

    with open("../indexed_data/simple_index.p", mode="wb") as index_file:
        pickle.dump(index, index_file)

    return compute_collection_statistics(index)


def save_spimi_blocks(doc_tokens):

    def to_docid(docno_tfs):
        for docno, tf in docno_tfs:
            if docno not in to_docid.mappings:
                to_docid.max += 1
                to_docid.mappings[docno] = to_docid.max
            yield to_docid.mappings[docno], tf

    to_docid.mappings = dict()
    to_docid.max = 0

    def sorted_by_docid(index):
        return ((token, to_docid(docno_tfs)) for token, docno_tfs in index.items())

    blocks = more_itertools.chunked(doc_tokens, blocksize)
    block_indexes = (sorted(sorted_by_docid(create_index(block)), key = lambda i : i[0]) for block in blocks)

    num_blocks = 0
    for blockno, block_index in enumerate(block_indexes, 1):
        with BlockFile(blockno, mode="w") as index_file:
            for token, docid_tfs in block_index:
                index_file.write(block_line(token, docid_tfs))
        num_blocks = blockno

    return num_blocks


def merge_spimi_blocks(num_blocks):

    def merge_docid_tfs(ds1, ds2):
        while True:
            d1 = next(ds1)
            d2 = next(ds2)
            if d1[0] == d2[0]:
                yield d1[0], d1[1] + d2[1]
            elif d1[0] < d2[0]:
                ds2.send(d2)
                yield d1
            else:
                ds1.send(d1)
                yield d2

    def merge_postings(p1, p2):
        return p1[0], merge_docid_tfs(iter(p1[1]), iter(p2[1]))

    block_files = {blockno: BlockFile(blockno, mode="r").open() for blockno in range(1, num_blocks + 1)}

    blocks = dict()
    for no, file in block_files.items():
        blocks[no] = from_block_line(file.readline())

    with open("../indexed_data/spimi_index.p", mode="w") as index_file:
        while blocks:

            min_token = min((posting[0] for posting in blocks.values()))
            min_token_blocknos = (no for no, posting in blocks.items() if posting[0] == min_token)
            min_token_postings = (posting for posting in blocks.values() if posting[0] == min_token)
            merged_posting = functools.reduce(merge_postings, min_token_postings)

            index_file.write(block_line(*merged_posting))

            for no in min_token_blocknos:
                blocks[no] = None

            for no, file in block_files.items():
                if blocks[no] is None:
                    blocks[no] = from_block_line(file.readline())
                    if blocks[no] is None:
                        del blocks[no]


def spimi(files, params):
    doc_tokens = itertools.chain.from_iterable(parse_and_process_file(file, params) for file in files)

    num_blocks = save_spimi_blocks(doc_tokens)
    merge_spimi_blocks(num_blocks)

    return None


def map_reduce(files, params):
    mapper_segments = [segmentize(file, params) for file in files]
    reducer_segments = collections.defaultdict(list)
    for segments in mapper_segments:
        for key, segment in segments.items():
            reducer_segments[key] += segment

    segment_indexes = {seg_key : reduce(segment) for seg_key, segment in reducer_segments.items()}

    statistics = compute_collection_statistics(segment_indexes)

    # save indexes, simple strategy: using pickle
    for seg_key in segment_keys:
        with SegmentFile(seg_key, mode="wb") as segment_file:
            pickle.dump(segment_indexes.get(seg_key), segment_file)

    return statistics


def main():
    params = parse_args()

    # get all files in specified directories recursively
    files = itertools.chain.from_iterable(
        glob.iglob('{}/**/*'.format(pattern), recursive=True)
        for pattern in params.patterns)

    shutil.rmtree("../indexed_data", ignore_errors=True)
    os.makedirs("../indexed_data", exist_ok=True)

    statistics = None
    if params.indexing_method == "simple":
        statistics = simple(files, params)
    if params.indexing_method == "spimi":
        statistics = spimi(files, params)
    if params.indexing_method == "map_reduce":
        statistics = map_reduce(files, params)

    # save statistics
    with open("../indexed_data/statistics.p", "wb") as stat_file:
        pickle.dump(statistics, stat_file)

    # save params so that the search script knows how to process query tokens
    with open("../indexed_data/settings.p", "wb") as settings_file:
        pickle.dump(params, settings_file)


if __name__ == '__main__':
    main()
