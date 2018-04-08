import heapq
import marshal
import operator
import os
from functools import reduce
from itertools import groupby

import more_itertools

from air18.index.common import create_index
from air18.util.paths import INDEX_BASE, SPIMI_INDEX_PATH, SPIMI_INDEX_INDEX_PATH
from air18.util.progress import ProgressBar

# results in block files of about 50 MB and 3 GB of peak memory usage
BLOCK_SIZE = 10000000


def block_line(token, docid_tfs):
    return token + ":" + ",".join(("%s-%s" % docid_tf for docid_tf in docid_tfs)) + "\n"


def from_block_line(line):
    if line == "":
        return None

    split_line = line.split(":")
    token = split_line[0]
    docid_tfs = [tuple(int(t) for t in docid_tf.split("-")) for docid_tf in split_line[1].split(",")]
    return token, docid_tfs


class BlockFile:
    def __init__(self, blocknumber, mode="rb"):
        self.filename = os.path.join(INDEX_BASE, "spimi_tmp_index_{}.txt".format(blocknumber))
        self.blocknumber = blocknumber
        self.mode = mode

    def open(self):
        self.block_file = open(self.filename, self.mode)
        return self.block_file

    def __enter__(self):
        return self.open()


    def __exit__(self, *args):
        self.block_file.close()


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