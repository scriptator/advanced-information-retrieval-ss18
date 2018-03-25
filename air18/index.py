#!/usr/bin/env python3

import argparse
import os
import collections
import re
import xml.etree.ElementTree as ET
import pickle

from nltk.stem import PorterStemmer, WordNetLemmatizer

from air18.segments import segment_key, segment_keys


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


def parse_and_process_file(file, params):
    doc_tokens = []

    with open(file) as f:
        root = ET.fromstringlist(["<ROOT>", f.read(), "</ROOT>"])
        for doc in root.findall("DOC"):
            docno = doc.find("DOCNO").text.strip()
            text = "\n".join(doc.find("TEXT").itertext())

            doc_tokens += [(docno, token) for token in parse_and_process_text(text, params)]

    return doc_tokens


def parse_and_process_text(text, params):
    # tokenize, very simple strategy: split on all non-alphanumeric characters
    tokens = re.split('[^a-zA-Z0-9]', text)
    tokens = list(filter(None, tokens))

    # case folding, simple strategy: all words to lowercase
    if params.case_folding:
        tokens = [token.lower() for token in tokens]

    # removing stop words
    if params.stop_words:
        from air18.stopwords import stop_words_en
        tokens = [token for token in tokens if token not in stop_words_en]

    # stemming
    if params.stemming:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(token) for token in tokens]

    # lemmatization
    if params.lemmatization:
        lemmatizer = WordNetLemmatizer()
        tokens = [lemmatizer.lemmatize(token) for token in tokens]

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


def map(file, params):
    segments = collections.defaultdict(list)

    doc_tokens = parse_and_process_file(file, params)
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

    mapper_segments = [map(file, params) for file in files]

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
        pickle.dump(segment_indexes.get(seg_key), open("index_" + seg_key + ".p", "wb"))


if __name__ == '__main__':
    main()
