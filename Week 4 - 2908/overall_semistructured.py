
from mrjob.job import MRJob
import json

class OverAllAmazon(MRJob):
  def mapper(self, _, value):
    line = json.loads(value)
    overall = float(line['overall'])
    if overall <= 2.5:
      yield "overall_less_2.5", 1
    else:
      yield "overall_greater_2.5", 1

  def reducer(self, key, value):
    yield key, sum(value)

if __name__ == '__main__':
  OverAllAmazon.run()
