#!/usr/bin/env python3

import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("topic", type=argparse.FileType,
                        help="the topic file containing the query")

    subparsers = parser.add_subparsers(dest="similarity_function", title="similarity function")
    subparsers.required = True

    subparsers.add_parser("tf-idf", help="use TF-IDF")

    bm25_parser = subparsers.add_parser("bm25", help="use BM25")
    bm25_parser.add_argument("b", help="b parameter")
    bm25_parser.add_argument("k1", help="k1 parameter")

    bm25va_parser = subparsers.add_parser("bm25va", help="use BM25 Verboseness Fission Variant")
    bm25va_parser.add_argument("k1", help="k1 parameter")

    return parser.parse_args()


def main():
    params = parse_args()
    # TODO


if __name__ == '__main__':
    main()