from mrjob.job import MRJob, MRStep
#import spacy
#import en_core_web_sm
from time import time

class NERJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_sort)
        ]

    def mapper_get_data(self, _, line):
        nlp = en_core_web_sm.load()
        doc = nlp(line.strip())
        for ent in doc.ents:
            yield ent.label_, 1

    def combiner_count(self, label, counts):
        yield (label, sum(counts))

    def reducer_count(self, label, counts):
        yield None, (sum(counts), label)

    def reducer_sort(self, _, label_count_pairs):
        # each itme of label_count_pairs is (count, label)
        # so yielding one results in key=counts, value=label
        for lc in sorted(label_count_pairs, reverse=True):
            yield lc


if __name__ == "__main__":
    print('start')
    t = time()
    NERJob.run()
    t_e = time()
    print(t_e-t, 'seconds')

