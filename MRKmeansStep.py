"""
.. module:: MRKmeansStep

MRKmeansStep
*************

:Description: MRKmeansStep

    Defines the MRJob MapReduce Steps

:Authors: bejar

:Version: 

:Created on: 17/07/2017 7:42 

"""

from collections import Counter
from itertools import chain
from mrjob.job import MRJob
from mrjob.step import MRStep

__author__ = 'bejar'


class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        '''
        Compute here the Jaccard similarity between  a prototype and a document.
        prot should be a list of pairs (word, probability).
        doc should be a list of words.
        Words must be alphabeticaly ordered.
        The result should be always a value in the range [0,1].

                Parameters:
                        self (): ...
                        prot (list): list of pairs [(word, probability)]
                        doc (list): list of words
                
                Returns:
                        jaccard_similarity (float): value in the range [0,1]
        '''

        prot = dict(prot)

        ordered_doc = sorted(doc)

        # Compute dot product
        dot_product = sum(
            prot.get(word, 0) * 1
            for word in ordered_doc
        )

        # Compute norms
        prototype_norm_sq = sum(value ** 2 for value in prot.values())
        document_norm_sq = len(doc)

        # Compute Jaccard similarity
        jaccard_similarity = dot_product / (prototype_norm_sq + document_norm_sq - dot_product)

        return jaccard_similarity


    def configure_args(self):
        """
        Additional configuration flag to get the prototypes files

        :return: None
        """
        super(MRKmeansStep, self).configure_args()
        self.add_file_arg('--prot')


    def load_data(self):
        """
        Loads the current cluster prototypes

        :return: None
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp


    def assign_prototype(self, _, line):
        '''
        This is the mapper it should compute the closest prototype to a document.
        Words should be sorted alphabetically in the prototypes and the documents.
        This function has to return at list of pairs (prototype_id, document words).
        You can add also more elements to the value element, for example the document_id.

                Parameters:
                        self (): ...
                        _ (): ...
                        line (str): line from input file containing docId and list of words
                
                Returns:
                        a pair (assigned_cluster, (doc, lwords)) where:
                                - assigned_cluster = clusterId assigned to the document
                                - doc = docId
                                - lwords = list of words in the document
        '''
        # extact data by splitting line 
        doc, words = line.split(':')
        lwords = words.split()

        # initializing comparison and assignment parameters
        max_score = float('-inf') 
        assigned_cluster = None

        # iterate through each prot and assign to most similar one
        for cluster, prototype in self.prototypes.items():
            score = self.jaccard(prototype, lwords)
            if score > max_score:
                max_score = score
                assigned_cluster = cluster

        yield (assigned_cluster, (doc, lwords))


    def aggregate_prototype(self, key, values):
        '''
        Input is cluster and all the documents it has assigned.
        Outputs should be at least a pair (cluster, new prototype).
        It should receive a list with all the words of the documents assigned for a cluster.
        The value for each word has to be the frequency of the word divided by the number of documents assigned to the cluster.
        Words are ordered alphabetically but you will have to use an efficient structure to compute the frequency of each word.

                Parameters:
                        self (): ...
                        key (str): assigned clusterId of doc by mapper
                        values (list): list of tuples (docId, words) assigned to clusterId

                Returns:
                        a pair (key, (assigned_docs, new_prototype)) where:
                                - key = clusterId
                                - assigned_docs = list of all docId assigned to clusterId
                                - new_prototype = list of top words by sorted by descending frequency/n_docs
        '''

        # Step 1: Initialize variables
        assigned_docs = []
        word_count = Counter()
        doc_count = 0

        # Step 2: Consume the generator
        for doc_id, words in values:
            assigned_docs.append(doc_id)
            word_count.update(words)
            doc_count += 1

        # Step 3: Calculate frequency normalized by number of documents
        word_frequencies = {word: count / doc_count for word, count in word_count.items()}

        # Step 4: Sort words by descending frequency, then alphabetically
        new_prototype = sorted(word_frequencies.items(), key=lambda x: (-x[1], x[0]))

        yield (key, (assigned_docs, new_prototype))


    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
                ]


if __name__ == '__main__':
    MRKmeansStep.run()