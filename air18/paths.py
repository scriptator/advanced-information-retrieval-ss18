import os

INDEX_BASE=os.path.expanduser("~/.air18/index/")

SETTINGS_FILEPATH = os.path.join(INDEX_BASE, "settings.p")
STATISTICS_FILEPATH = os.path.join(INDEX_BASE, "statistics.p")
DOCUMENT_STATISTICS_FILEPATH = os.path.join(INDEX_BASE, "document_statistics.p")

SIMPLE_INDEX_PATH = os.path.join(INDEX_BASE, "simple_index.p")