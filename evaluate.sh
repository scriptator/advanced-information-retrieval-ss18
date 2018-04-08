#!/bin/bash

set -e

rm -rf evaluation_results
mkdir evaluation_results

# TF-IDF
echo "Testing TF-IDF"
python3 -m air18.search tf-idf > evaluation_results/tf-idf.trec
trec_eval -q -m map -c resources/qrels.trec8.adhoc.parts1-5 evaluation_results/tf-idf.trec | tee evaluation_results/evaluation_tf-idf.txt
echo

# BM25
echo "Testing BM25"
python3 -m air18.search bm25 > evaluation_results/bm25.trec
trec_eval -q -m map -c resources/qrels.trec8.adhoc.parts1-5 evaluation_results/bm25.trec | tee evaluation_results/evaluation_bm25.txt
echo

# BM25 Verboseness Fission Variant
echo "Testing BM25 Verboseness Fission"
python3 -m air18.search bm25va > evaluation_results/bm25va.trec
trec_eval -q -m map -c resources/qrels.trec8.adhoc.parts1-5 evaluation_results/bm25va.trec | tee evaluation_results/evaluation_bm25va.txt
