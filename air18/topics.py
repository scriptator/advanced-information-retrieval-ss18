import functools
import os
import re

from air18.token import tokenize

NUMBER_REGEX = r"<num> Number: (4\d\d)"
TITLE_REGEX = r"<title> (.*)"


def parse_topics(topics_file, case_folding=False, stop_words=False, stemming=False,
                 lemmatization=False):
    topics_string = topics_file.read()
    numbers = re.findall(NUMBER_REGEX, topics_string)
    titles = re.findall(TITLE_REGEX, topics_string)

    if len(numbers) != len(titles):
        raise ValueError("Topic file is invalid. Number of <num> and <title> tags must be equal")

    tokenize_fun = functools.partial(tokenize, case_folding=case_folding,
                                     stop_words=stop_words, stemming=stemming,
                                     lemmatization=lemmatization)
    title_tokens = map(list, map(tokenize_fun, titles))
    return dict(zip(numbers, title_tokens))
