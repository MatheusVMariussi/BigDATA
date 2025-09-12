
from mrjob.job import MRJob
from mrjob.job import MRStep
import re

class WordCountCombined(MRJob):

  def mapper(self, _, value):
    words = re.findall("[a-z]+", value.lower())
    for word in words:
      yield word, 1

  def reducer(self, key, value):
    yield key, sum(value)

# o combiner é exatamente o mesmo codigo do reducer, logo:
  def steps(self):
    return [MRStep(mapper=self.mapper,
                   combiner=self.reducer,
                   reducer=self.reducer)]

if __name__ == '__main__':
  WordCountCombined.run()
     
