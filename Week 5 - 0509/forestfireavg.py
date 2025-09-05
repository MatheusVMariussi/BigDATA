
from mrjob.job import MRJob

class ForestFireAvarageTemp(MRJob):
  def mapper(self, _, value):
    fields = value.split(",")
    if len(fields) == 13:
      try:
        temperature = float(fields[8])
        yield "Temp", [temperature, 1] #chave comum e valor composto (num,1)
      except ValueError:
        pass

  def reducer(self, key, value):
    sum_values = 0
    count = 0
    for v in value:
      sum_values += v[0]
      count += v[1]
    avg = sum_values / count
    yield "Average", avg

if __name__ == '__main__':
  ForestFireAvarageTemp.run()
