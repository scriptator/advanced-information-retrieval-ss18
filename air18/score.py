from math import log

from air18.statistics import CollectionStatistics


def tf_idf(tf_td, df_t, collection_statistics: CollectionStatistics, **kwargs):
    return log(1 + tf_td) * log(collection_statistics.num_documents / df_t)


def bm25(tf_td, df_t, collection_statistics: CollectionStatistics, **kwargs):
    raise NotImplementedError()


def bm25_verboseness_fission(tf_td, df_t, collection_statistics: CollectionStatistics, **kwargs):
    raise NotImplementedError()