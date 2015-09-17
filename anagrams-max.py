# Find the word with the maximum number of anagrams
# solution is expressed as a multi-step MR flow
#
# Map-1:     (sorted-word, word)
# Reduce-1:  (#, [anagrams])
# Reduce-2:  max 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAnagram(MRJob):

    def word2letters(self, _, word):
        # Convert word into a list of characters, sort them, and convert
        # back to a string.
        letters = list(word)
        letters.sort()

        # Key is the sorted word, value is the regular word.
        yield letters, word

    def collect_anagrams(self, _, words):
        # Get the list of words containing these letters.
        anagrams = [w for w in words]

        # Only yield results if there are at least two words which are
        # anagrams of each other.
        # But this time all values are on the same key!
        if len(anagrams) >= 2:
            yield None, (len(anagrams), anagrams)

    def find_max(self, _, num_anagrams):
        yield None, max(num_anagrams)

    # create the work flow
    def steps(self):
        return [
            MRStep(mapper=self.word2letters,
                   reducer=self.collect_anagrams),
            MRStep(reducer=self.find_max)
           ]

if __name__ == "__main__":
    MRAnagram.run()

        
