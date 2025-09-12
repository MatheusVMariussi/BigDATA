#temp média por mes usando combiner

from mrjob.job import MRJob
from mrjob.step import MRStep

class ForestFireAvgTempCombiner(MRJob):
  def mapper(self, _, value):
    fields = value.split(',')
    if len(fields) == 13:
      try:
        month = fields[2]
        temp = float(fields[8])
        yield month, [temp, 1]
      except ValueError:
        pass

  def combiner(self, key, value):
    partial_sum = 0
    partial_count = 0
    for v in value:
      partial_sum += v[0]
      partial_count += v[1]
    #ao invez de calcular a media, o combiner retorna somente as somas
    yield key, (partial_sum, partial_count)

  def reducer(self, key, value):
    sum_values = 0
    count = 0
    for v in value:
      sum_values += v[0]
      count += v[1]
    avg_temp = sum_values/count
    yield key, avg_temp

  def steps(self):
    return [MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)]
                   
if __name__ == '__main__':
  ForestFireAvgTempCombiner.run()
