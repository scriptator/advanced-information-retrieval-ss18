import itertools
import json
from xml.etree import ElementTree as ET


# https://stackoverflow.com/a/24527424/7594528
def chunks(iterable, size=10):
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def parse_xml(file):
    root = ET.fromstringlist(["<ROOT>", file.read(), "</ROOT>"])
    for doc in root.findall("DOC"):
        docno = doc.find("DOCNO").text.strip()
        text = "\n".join(doc.find("TEXT").itertext())
        # Documents that do not have a <TEXT> tag can be ignored
        if text != "":
            yield docno, text


def parse_json(file):
    doclist = json.load(file)
    for doc in doclist:
        # Documents that do not have a <TEXT> tag can be ignored
        if doc["text"] is not None:
            yield doc["docno"], doc["text"]
