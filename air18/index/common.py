import collections
from functools import partial
from itertools import chain
from typing import Union, Dict, Tuple

from air18.util.parsing import parse_json, parse_xml
from air18.index.statistics import CollectionStatistics
from air18.index.tokens import air_tokenize


def parse_and_process_file(file, params, docid_docno_mapping: Union[Dict[int, str], None],
                           doc_stats: Dict[int, Tuple],
                           collection_statistics: CollectionStatistics):
    """
    Parse and tokenize file.

    :param file: path to the input file
    :param params: argparse params
    :param docid_docno_mapping: a dictionary containing the mapping, or None if mapping should be disabled
    :param doc_stats: a dictionary filled with statistics per document
    :param collection_statistics:
    :return: nothing, yield Tuples (docid, token)
    """
    if docid_docno_mapping is None:
        map_docid = False
    else:
        map_docid = True

    print("Parsing file {}".format(file))
    with open(file, encoding="iso-8859-1") as f:
        if file.endswith(".json"):
            data = parse_json(f)
        else:
            data = parse_xml(f)

        for docno, text in data:
            if map_docid:
                docid = collection_statistics.num_documents
                docid_docno_mapping[docid] = docno
            else:
                docid = docno

            dl = 0
            unique_terms = set()
            for token in air_tokenize(text, params.case_folding, params.stop_words,
                                      params.stemming, params.lemmatization):
                dl += 1
                unique_terms.add(token)
                yield (docid, token)

            if len(unique_terms) > 0:
                # save document statistics
                avgtf = dl / len(unique_terms)
                doc_stats[docid] = (dl, avgtf)

                # update collection statistics
                collection_statistics.total_doc_length += dl
                collection_statistics.sum_avgtf += avgtf
                collection_statistics.num_documents += 1


def create_token_stream(files, params):
    doc_stats = {}
    docid_docno_mapping = {}
    statistics = CollectionStatistics()
    parse_fn = partial(parse_and_process_file, params=params,
                       docid_docno_mapping=docid_docno_mapping,
                       doc_stats=doc_stats,
                       collection_statistics=statistics)
    token_stream = chain.from_iterable(parse_fn(file=file) for file in files)
    return token_stream, doc_stats, docid_docno_mapping, statistics


def create_index(doc_tokens: Tuple[Union[str, int], str]):
    index = collections.defaultdict(list)

    # invert step: add everything to a list
    for docid, token in doc_tokens:
        index[token].append(docid)

    # convert list to tuples (docid, tf)
    for token, postings in index.items():
        counter = collections.Counter(postings)
        index[token] = sorted(counter.items())

    # convert defaultdict to dict
    return dict(index)