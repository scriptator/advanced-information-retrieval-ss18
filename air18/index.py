#!/usr/bin/env python3

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", nargs="+", help="The file in JSON format to index")


def main():
    params = parse_args()

    # TODO


if __name__ == '__main__':
    main()