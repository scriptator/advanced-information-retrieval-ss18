import functools
import json
import os
import re
from xml.etree import ElementTree as ET

from air18.tokens import air_tokenize

NUMBER_REGEX = r"<num> Number: (4\d\d)"
TITLE_REGEX = r"<title> (.*)"


def parse_topics(topics_file, case_folding=False, stop_words=False, stemming=False,
                 lemmatization=False):
    topics_string = topics_file.read()
    numbers = re.findall(NUMBER_REGEX, topics_string)
    titles = re.findall(TITLE_REGEX, topics_string)

    if len(numbers) != len(titles):
        raise ValueError("Topic file is invalid. Number of <num> and <title> tags must be equal")

    tokenize_fun = functools.partial(air_tokenize, case_folding=case_folding,
                                     stop_words=stop_words, stemming=stemming,
                                     lemmatization=lemmatization)
    title_tokens = map(list, map(tokenize_fun, titles))
    return dict(zip(numbers, title_tokens))


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