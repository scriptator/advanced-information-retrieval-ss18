from math import log

from air18.statistics import CollectionStatistics


def tf_idf(tf_td, idf_t, collection_statistics: CollectionStatistics, **kwargs):
    return log(1 + tf_td) * idf_t


def bm25(tf_td, idf_t, collection_statistics: CollectionStatistics, k1: float,
         b: float, dl: int, **kwargs):
    return idf_t * tf_td * (k1 + 1) / (tf_td + k1 * (1 - b + b * dl / collection_statistics.avg_doc_length))


def bm25_verboseness_fission(tf_td, idf_t, collection_statistics: CollectionStatistics, **kwargs):
    raise NotImplementedError()