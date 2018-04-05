class CollectionStatistics:

    def __init__(self, num_documents=0, total_doc_length=0):
        self.num_documents = num_documents
        self.total_doc_length = total_doc_length

    @property
    def avg_doc_length(self):
        return self.total_doc_length / self.num_documents
