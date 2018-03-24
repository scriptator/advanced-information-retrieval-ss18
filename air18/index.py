#!/usr/bin/env python3

import argparse
import os
import collections
import re
import xml.etree.ElementTree as ET


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("files",
                        nargs="+",
                        help="the directory (recursively) containing files or a single file in XML format to index")
    parser.add_argument("--case-folding", action="store_true", help="apply case folding")
    parser.add_argument("--stop-words", action="store_true", help="remove stop words")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--stemming", action="store_true", help="apply stemming")
    group.add_argument("--lemmatization", action="store_true", help="apply lemmatization")

    return parser.parse_args()


def parse_and_process_file(file):
    doc_tokens = []

    with open(file) as f:
        root = ET.fromstringlist(["<ROOT>", f.read(), "</ROOT>"])
        for doc in root.findall("DOC"):
            docno = doc.find("DOCNO").text.strip()
            text = "\n".join(doc.find("TEXT").itertext())

            doc_tokens += [(docno, token) for token in parse_and_process_text(text)]

    return doc_tokens


def parse_and_process_text(text):
    # tokenize, very simple strategy: split on all non-alphanumeric characters
    tokens = re.split('[^a-zA-Z0-9]', text)
    tokens = list(filter(None, tokens))

    # TODO: special strings?

    # TODO: techniques combinations:
    # case folding
    # removing stop words
    # stemming (library)
    # lemmatization (library)

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


segment_keys = ['a', 'f', 'k', 'p', 'u', 'z', 256]


def segment_key(doc_token):
    t = doc_token[1][0].lower()
    for segment_highest_key in segment_keys:
        if t <= segment_highest_key:
            return segment_highest_key

    # ISO 8859-1 encoding should not allow values higher 256
    raise IndexError


def map(file):
    segments = collections.defaultdict(list)

    doc_tokens = parse_and_process_file(file)
    for doc_token in doc_tokens:
        segments[segment_key(doc_token)].append(doc_token)

    return segments


def reduce(segment):
    return create_index(segment)


def main():
    params = parse_args()

    # get all files in specified directories recursively
    files = []
    for file in params.files:
        if os.path.isfile(file):
            files.append(file)
        elif os.path.isdir(file):
            for _, _, dirfiles in os.walk(file):
                files += dirfiles
        else:
            raise FileNotFoundError

    mapper_segments = [map(file) for file in files]

    reducer_segments = collections.defaultdict(list)
    for segments in mapper_segments:
        for key, segment in segments.items():
            reducer_segments[key] += segment

    segment_indexes = [reduce(segment) for _, segment in reducer_segments.items()]

    # print index
    for segment_index in segment_indexes:
        for token, docs in segment_index.items():
            print("token: " + token)
            for doc in docs:
                print(doc)

    # TODO: save index


if __name__ == '__main__':
    main()
