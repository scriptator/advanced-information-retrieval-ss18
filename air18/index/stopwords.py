import os

from air18.util.paths import RESOURCES_DIR

STOPWORDS_FILE_PATH = os.path.join(RESOURCES_DIR, "nltk_stopwords_en.txt")
stop_words_en = set(open(STOPWORDS_FILE_PATH, "r").read().splitlines())
