import os

from air18.index.common import parse_and_process_file, create_index
from air18.util.paths import INDEX_BASE
from air18.index.statistics import CollectionStatistics

segment_keys = ['a', 'f', 'k', 'p', 'u', 'z', 'o']


def segment_key(doc_token):
    t = doc_token[1][0].lower()
    for segment_highest_key in segment_keys:
        if t <= segment_highest_key:
            return segment_highest_key

    return segment_keys[-1]


class SegmentFile:
    def __init__(self, key, mode="rb"):
        self.filename = os.path.join(INDEX_BASE, "index_{}.p".format(key))
        self.key = key
        self.mode = mode

    def __enter__(self):
        self.segment_file = open(self.filename, self.mode)
        return self.segment_file

    def __exit__(self, *args):
        self.segment_file.close()


def air_map(file, params):
    doc_stats = {}
    collection_statistics = CollectionStatistics()
    token_stream = parse_and_process_file(file, params, docid_docno_mapping=None,
                                          doc_stats=doc_stats,
                                          collection_statistics=collection_statistics)
    return token_stream, doc_stats, collection_statistics


def air_reduce(segment):
    return create_index(segment)