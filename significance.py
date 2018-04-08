# coding: utf-8
import re
from scipy import stats


def parse(path):
    maps, p10s = [], []
    with open(path) as evalfile:
        l1 = evalfile.readline()
        while l1 != "":
            v1 = float(re.split(r'\t+', l1)[2].strip())
            maps.append(v1)
            l1 = evalfile.readline()
    return maps[:-1]


def significance_test(a, b, threshold=0.05):
    test_result = stats.ttest_rel(a, b)
    if test_result.pvalue > threshold:
        print("PValue = {} --> The null hypothesis, that the distributions are equal cannot be rejected (threshold {})".format(test_result.pvalue, threshold))
    else:
        print("PValue = {} --> The null hypothesis can be rejected (threshold {})".format(test_result.pvalue, threshold))
    print()

maps_tf_idf = parse("evaluation_results/evaluation_tf-idf.txt")
maps_bm25 = parse("evaluation_results/evaluation_bm25.txt")
maps_bm25va = parse("evaluation_results/evaluation_bm25va.txt")

print("TF-IDF vs BM25")
significance_test(maps_tf_idf, maps_bm25)

print("TF-IDF vs BM25VA")
significance_test(maps_tf_idf, maps_bm25va)

print("BM25 vs BM25VA")
significance_test(maps_bm25, maps_bm25va)
