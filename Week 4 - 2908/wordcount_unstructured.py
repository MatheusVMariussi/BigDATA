
import re
from mrjob.job import MRJob

class WordCount(MRJob):

  # tanto o mapper quanto o reducer, recebem (chave, valor) como entrada
  def mapper(self, _, value):
    words = re.findall("[a-z]+", value.lower())
    for word in words: 
      yield word, 1

  def reducer(self, key, value):
    sum = 0
    for v in value:
      sum+=1
    yield key, sum

if __name__ == '__main__':
  WordCount.run()
