import os

INDEX_BASE=os.path.expanduser("~/.air18/index/")

SETTINGS_FILEPATH = os.path.join(INDEX_BASE, "settings.p")
STATISTICS_FILEPATH = os.path.join(INDEX_BASE, "statistics.p")
DOCUMENT_STATISTICS_FILEPATH = os.path.join(INDEX_BASE, "document_statistics.p")
DOCID_DOCNO_MAPPING = os.path.join(INDEX_BASE, "docid_docno_mapping.p")

SIMPLE_INDEX_PATH = os.path.join(INDEX_BASE, "simple_index.p")

SPIMI_INDEX_PATH = os.path.join(INDEX_BASE, "spimi_index.txt")
SPIMI_INDEX_INDEX_PATH = os.path.join(INDEX_BASE, "spimi_index_index.p")

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "resources")
DEFAULT_TOPIC_FILE=os.path.join(RESOURCES_DIR, "topicsTREC8Adhoc.txt")
