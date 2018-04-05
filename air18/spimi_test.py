import unittest
import itertools

from air18.index import save_spimi_blocks, merge_spimi_blocks


class SpimiTest(unittest.TestCase):

    def test_spimi(self):
        iter = itertools.repeat((('1aa', 'a'),
                                 ('1aa', 'b'),
                                 ('2bb', 'a'),
                                 ('3cc', 'another'),
                                 ('1aa', 'test'),
                                 ('2bb', 'case'),
                                 ('3cc', 'c'),
                                 ('2bb', 'c'),
                                 ('3cc', 'e'),
                                 ('1aa', 'fghijklmnop')), 15)
        doc_tokens = itertools.chain.from_iterable(iter)

        num_blocks = save_spimi_blocks(doc_tokens)
        merge_spimi_blocks(num_blocks)

