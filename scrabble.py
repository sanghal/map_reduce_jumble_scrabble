from mrjob.job import MRJob

scores = {"a": 1, "c": 3, "b": 3, "e": 1, "d": 2, "g": 2,
          "f": 4, "i": 1, "h": 4, "k": 5, "j": 8, "m": 3,
          "l": 1, "o": 1, "n": 1, "q": 10, "p": 3, "s": 1,
          "r": 1, "u": 1, "t": 1, "w": 4, "v": 4, "y": 4,
          "x": 8, "z": 10}

class MRScrabble(MRJob):

    def mapper(self, _, word):
      score = 0
      self.tiles = list('abcdefg')
      letters = list(word)
      letters.sort() 

      for letter in letters:
        if letter in self.tiles:
          self.tiles.remove(letter)
          score += int(scores[letter])
          

        else:
          return


      yield None, (score,word)


    def reducer(self, _, words):

        result = [w for w in words]

        # Only yield results if there are at least two words which are
        # anagrams of each other.
        yield None, result


if __name__ == "__main__":
    MRScrabble.run()

        

