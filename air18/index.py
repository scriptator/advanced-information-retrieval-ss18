#!/usr/bin/env python3

import argparse


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


def main():
    params = parse_args()

    # TODO: read in from files

    # TODO: basic tokenizer

    # TODO: special strings?

    # TODO: techniques combinations:
    # case folding
    # removing stop words
    # stemming (library)
    # lemmatization (library)

    # TODO: build inverted index:
    # Simple posting list, Hash or B-Tree dictionary
    # Single Pass In Memory Indexing
    # Map-Reduce?

    # TODO: save index

if __name__ == '__main__':
    main()
