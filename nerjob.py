# from mrjob.job import MRJob, MRStep
# import spacy
# import en_core_web_sm
# from time import time
#
# class NERJob(MRJob):
#
#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_get_data,
#                    combiner=self.combiner_count,
#                    reducer=self.reducer_count),
#             MRStep(reducer=self.reducer_sort)
#         ]
#
#     def mapper_get_data(self, _, line):
#         try:
#             nlp = en_core_web_sm.load()
#             doc = nlp(line.strip())
#             for ent in doc.ents:
#                 if ent.label_ != '' and ent.label_ is not None:
#                     yield ent.label_, 1
#         except Exception as e:
#             #print(e)
#             #raise e
#
#     def combiner_count(self, label, counts):
#         try:
#             if label is not None and label != '' and counts is not None:
#                 yield (label, sum(counts))
#         except Exception as e:
#             pass
#             #print(e)
#             #raise e
#
#     def reducer_count(self, label, counts):
#         try:
#             if label is not None and label != '' and counts is not None:
#                 yield None, (sum(counts), label)
#         except Exception as e:
#             pass
#             #print(e)
#             #raise e
#
#     def reducer_sort(self, _, label_count_pairs):
#         # each itme of label_count_pairs is (count, label)
#         # so yielding one results in key=counts, value=label
#         try:
#             if label_count_pairs is not None:
#                 for lc in sorted(label_count_pairs, reverse=True):
#                     yield lc
#         except Exception as e:
#             pass
#             #print(e)
#             #raise e
#
#
# if __name__ == "__main__":
#     t = time()
#     NERJob.run()
#     t_e = time()
#     print(t_e-t, 'seconds')
#
#


from mrjob.job import MRJob, MRStep
import spacy
import en_core_web_sm
from time import time

class NERJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data,
                   #combiner=self.combiner_count,
                   reducer=self.reducer_ct),
            MRStep(reducer=self.reducer_count),
            MRStep(reducer=self.reducer_sort)
        ]

    def mapper_get_data(self, _, line):
        try:
            nlp = en_core_web_sm.load()
            doc = nlp(line.strip())
            for ent in doc.ents:
                if ent.label_ != '' and ent.label_ is not None:
                    yield ent.label_, 1
        except Exception as e:
            pass
            #print(f'step 1 mapper phase, line is:{line}, error is: {e}')

    def reducer_ct(self, label, counts):
        try:
            if label is not None and label != '' and counts is not None:
                yield (label, sum(counts))
        except Exception as e:
            pass
            #print(e)
            #raise e

    def reducer_count(self, label, counts):
        try:
            if label is not None and label != '' and counts is not None:
                yield None, (sum(counts), label)
        except Exception as e:
            pass
            #print(f'step 1 reducer phase, label is:{label}, error is: {e}')

    def reducer_sort(self, _, label_count_pairs):
        # each itme of label_count_pairs is (count, label)
        # so yielding one results in key=counts, value=label
        try:
            if label_count_pairs is not None:
                for lc in sorted(label_count_pairs, reverse=True):
                    yield lc
        except Exception as e:
            pass
            #print(f'step 2 reducer phase, error is: {e}')


if __name__ == "__main__":
    t = time()
    NERJob.run()
    t_e = time()
    #print(t_e-t, 'seconds')