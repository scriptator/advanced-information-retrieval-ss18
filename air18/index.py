#!/usr/bin/env python3

import argparse
import glob
import json
import os
import collections
import re
import xml.etree.ElementTree as ET
import pickle

import itertools
from nltk.stem import PorterStemmer, WordNetLemmatizer

from air18.segments import segment_key, segment_keys, SegmentFile


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

    return parser.parse_args()


def parse_and_process_file(file, params):
    print("Parsing file {}".format(file))
    with open(file, encoding="iso-8859-1") as f:
            if file.endswith(".json"):
                data = parse_json(f)
            else:
                data = parse_xml(f)

            for docno, text in data:
                for token in parse_and_process_text(text, params):
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


def parse_and_process_text(text, params):
    # tokenize, very simple strategy: split on all non-alphanumeric characters
    tokens = re.split('[^a-zA-Z0-9]', text)
    tokens = filter(None, tokens)

    # case folding, simple strategy: all words to lowercase
    if params.case_folding:
        tokens = map(lambda token: token.lower(), tokens)

    # removing stop words
    if params.stop_words:
        from air18.stopwords import stop_words_en
        tokens = filter(lambda token: token not in stop_words_en, tokens)

    # stemming
    if params.stemming:
        stemmer = PorterStemmer()
        tokens = map(stemmer.stem, tokens)

    # lemmatization
    if params.lemmatization:
        lemmatizer = WordNetLemmatizer()
        tokens = map(lemmatizer.lemmatize, tokens)

    return tokens


def create_index(doc_tokens):
    # simple index without major performance considerations
    index = collections.defaultdict(set)

    for doc_token in doc_tokens:
        index[doc_token[1]].add(doc_token[0])

    for token in index:
        index[token] = sorted(index[token])

    # TODO: improvements:
    # Simple posting list, Hash or B-Tree dictionary
    # Single Pass In Memory Indexing

    return index


def segmentize(file, params):
    segments = collections.defaultdict(list)

    for doc_token in parse_and_process_file(file, params):
        segments[segment_key(doc_token)].append(doc_token)

    return segments


def reduce(segment):
    return create_index(segment)


def main():
    params = parse_args()

    # get all files in specified directories recursively
    files = itertools.chain.from_iterable(
        glob.iglob('{}/**/*'.format(pattern), recursive=True)
        for pattern in params.patterns)
    mapper_segments = [segmentize(file, params) for file in files]

    reducer_segments = collections.defaultdict(list)
    for segments in mapper_segments:
        for key, segment in segments.items():
            reducer_segments[key] += segment

    segment_indexes = {seg_key : reduce(segment) for seg_key, segment in reducer_segments.items()}

    # print indexes
    for _, segment_index in segment_indexes.items():
        for token, docs in segment_index.items():
            print("token: " + token)
            for doc in docs:
                print(doc)

    # save indexes, simple strategy: using pickle
    for seg_key in segment_keys:
        with SegmentFile(seg_key) as segment_file:
            pickle.dump(segment_indexes.get(seg_key), segment_file)


if __name__ == '__main__':
    main()
