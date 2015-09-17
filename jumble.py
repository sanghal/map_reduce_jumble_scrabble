from mrjob.job import MRJob

class MRAnagram(MRJob):

    def mapper(self, _, line):
        # When a mapper takes input from the command line the key is None
        # Since we don't care about the key we use _ as the variable
        # Convert word into a list of characters, sort them

        if len(line.split()) == 2:
            letters = list(line.split()[0])
            letters.sort()
            yield letters, "?" + line.split()[0]
            # letters.append(line.split()[0])
        else:
            letters = list(line)
            line = line.split()
            letters.sort()
            yield letters, line[0]
       

        # 'letters' is the key (sorted list of letters in word)
        # the read in word is the value
        

    def reducer(self, _, words):

        # Words is a generator.  Extract the list of words from it
        anagrams = [w for w in words]

        # Only yield results if there are at least two words which are
        # anagrams of each other.
        if anagrams[0][0] == "?":
            if len(anagrams) >= 2:
                yield len(anagrams), anagrams

if __name__ == "__main__":
    MRAnagram.run()

        