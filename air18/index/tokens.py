import re

import porterstemmer
from nltk import WordNetLemmatizer


def air_tokenize(text, case_folding=False, stop_words=False, stemming=False, lemmatization=False):
    # tokenize, simple strategy:
    # split on all non-alphanumeric characters
    # plus some adjustments for special word handling (email, strings with hyphens)
    tokens = re.split('[^a-zA-Z0-9.@]|\.[^a-zA-Z0-9]|\.$', text)
    tokens = filter(None, tokens)

    # case folding, simple strategy: all words to lowercase
    if case_folding:
        tokens = map(str.lower, tokens)

    # removing stop words
    if stop_words:
        from air18.index.stopwords import stop_words_en
        tokens = filter(lambda token: token not in stop_words_en, tokens)

    # stemming
    if stemming:
        stemmer = porterstemmer.Stemmer()
        tokens = map(stemmer, tokens)

    # lemmatization
    if lemmatization:
        lemmatizer = WordNetLemmatizer()
        tokens = map(lemmatizer.lemmatize, tokens)

    return tokens