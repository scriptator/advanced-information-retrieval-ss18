from math import log

from air18.statistics import CollectionStatistics


def tf_idf(tf_td, idf_t, collection_statistics: CollectionStatistics, **kwargs):
    return log(1 + tf_td) * idf_t


def bm25(tf_td, idf_t, collection_statistics: CollectionStatistics, k1: float,
         b: float, dl: int, **kwargs):
    B = (1 - b + b * dl / collection_statistics.avgdl)
    return idf_t * tf_td * (k1 + 1) / (tf_td + k1 * B)


def bm25_verboseness_fission(tf_td, idf_t, collection_statistics: CollectionStatistics,
                             k1, dl, avgtf, **kwargs):
    b = collection_statistics.b_verboseness_fission
    BVa = (1 - b) * (avgtf / collection_statistics.mavgtf )+ b * (dl / collection_statistics.avgdl)
    return idf_t * tf_td * (k1 + 1) / (tf_td + k1 * BVa)
