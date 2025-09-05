
from mrjob.job import MRJob
import re
import json

class WordCountAmazon(MRJob):
  def mapper(self, _, value):
    line = json.loads(value)
    review_text = line['reviewText']
    words = re.findall("[a-z]+", review_text.lower())
    for word in words:
      yield word, 1

  def reducer(self, key, value):
    yield key, sum(value)

if __name__ == '__main__':
  WordCountAmazon.run()
