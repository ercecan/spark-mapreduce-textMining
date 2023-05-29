from mrjob.job import MRJob
import spacy
import en_core_web_sm
nlp = en_core_web_sm.load()


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        try:
            if len(line) != 0:
                x = nlp(line)
                yield "chars", len(line)
                yield "words", len(line.split())
                yield "lines", 1
        except Exception as e:
            pass
            # print(f'mapper phase, line is:{line}, error is: {e}')

    def reducer(self, key, values):
        try:
            yield key, sum(values)
        except Exception as e:
            pass
            # print(f'reducer phase, key is:{key}, error is: {e}')


if __name__ == '__main__':
    # print('start word count')
    MRWordFrequencyCount.run()
