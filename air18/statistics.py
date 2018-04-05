class CollectionStatistics:

    def __init__(self, num_documents=0, total_doc_length=0, sum_avgtf=0):
        self.num_documents = num_documents
        self.total_doc_length = total_doc_length
        self.sum_avgtf = sum_avgtf
        self.avgdl = None
        self.mavgtf = None
        self.b_verboseness_fission = None

    def finalize(self):
        self.avgdl = self.total_doc_length / self.num_documents
        self.mavgtf = self.sum_avgtf / self.num_documents

        # actually this would not belong here but it is needed to optimize scoring
        self.b_verboseness_fission = 1 - (1 / self.mavgtf)
