import os


blocksize = 100


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
        os.makedirs("../indexed_data/", exist_ok=True)
        self.filename = "../indexed_data/index_{}.txt".format(blocknumber)
        self.blocknumber = blocknumber
        self.mode = mode

    def open(self):
        self.block_file = open(self.filename, self.mode)
        return self.block_file

    def __enter__(self):
        return self.open()


    def __exit__(self, *args):
        self.block_file.close()