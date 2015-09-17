# Calculate the largest number from a list of numbers

from mrjob.job import MRJob

class MRMax(MRJob):  

    # Very simple strategy to compute the max of a list of numbers
    # Yield all the numbers with the same key!
    
    def mapper(self, _, line):
        if line.strip()!='':
            yield(None, int(line))

    # all numbers with the same key are collected in 'values' :-)
    def reducer(self, _, values):
        yield None, max(values)

if __name__ == '__main__':
    MRMax.run()
