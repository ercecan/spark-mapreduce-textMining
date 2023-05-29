from mrjob.job import MRJob
import spacy
import en_core_web_sm
from time import time

class NERJob(MRJob):

    def mapper(self, _, line):
        nlp = en_core_web_sm.load()
        doc = nlp(line.strip())
        for ent in doc.ents:
            yield ent.label_, 1

    def reducer(self, label, counts):
        yield label, sum(counts)


if __name__ == "__main__":
    print('start')
    t = time()
    NERJob.run()
    t_e = time()
    print(t_e-t, 'seconds')