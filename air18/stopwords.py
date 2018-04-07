import os

STOPWORDS_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "resources", "nltk_stopwords_en.txt")
stop_words_en = set(open(STOPWORDS_FILE_PATH, "r").read().splitlines())
