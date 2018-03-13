import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", type=argparse.FileType,
                        help="The topic file containing the query")


def main():
    params = parse_args()
    # TODO


if __name__ == '__main__':
    main()