import os

from air18.paths import INDEX_BASE

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