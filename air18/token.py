import re

from nltk import PorterStemmer, WordNetLemmatizer


def tokenize(text, case_folding=False, stop_words=False, stemming=False,
             lemmatization=False):
    # tokenize, very simple strategy: split on all non-alphanumeric characters
    tokens = re.split('[^a-zA-Z0-9]', text)
    tokens = filter(None, tokens)

    # case folding, simple strategy: all words to lowercase
    if case_folding:
        tokens = map(lambda token: token.lower(), tokens)

    # removing stop words
    if stop_words:
        from air18.stopwords import stop_words_en
        tokens = filter(lambda token: token not in stop_words_en, tokens)

    # stemming
    if stemming:
        stemmer = PorterStemmer()
        tokens = map(stemmer.stem, tokens)

    # lemmatization
    if lemmatization:
        lemmatizer = WordNetLemmatizer()
        tokens = map(lemmatizer.lemmatize, tokens)

    return tokens