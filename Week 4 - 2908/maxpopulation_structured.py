
from mrjob.job import MRJob

class maxpopulation_structured(MRJob):
  def mapper(self, _, value):
    fields = value.split(',')
    # verifica se tem todas as colunas
    if len(fields) == 9:
      try:
        population = float(fields[5])
        yield "population", population
      except ValueError:
        pass #ignora se nao for float
      

  def reducer(self, key, value):
    yield "max_population", max(value)

if __name__ == '__main__':
  maxpopulation_structured.run()
