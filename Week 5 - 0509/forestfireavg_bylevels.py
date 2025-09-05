
from mrjob.job import MRJob

class ForestFireAvarageByLevels(MRJob):
    def mapper(self, _, value):
        fields = value.split(",")
        if len(fields) == 13:
            try:
                temperature = float(fields[8])
                humidity = float(fields[9])
                wind = float(fields[10])
                if wind < 5:
                  if humidity < 30:
                    yield "low wind, low humidity", (temperature, 1)
                  elif humidity >= 30 and humidity <= 60:
                    yield "low wind, medium humidity", (temperature, 1)
                  else:
                    yield "low wind, high humidity", (temperature, 1)
                elif wind >= 5 and wind <= 10:
                  if humidity < 30:
                    yield "medium wind, low humidity", (temperature, 1)
                  elif humidity >= 30 and humidity <= 60:
                    yield "medium wind, medium humidity", (temperature, 1)
                  else:
                    yield "medium wind, high humidity", (temperature, 1)
                else:
                  if humidity < 30:
                    yield "high wind, low humidity", (temperature, 1)
                  elif humidity >= 30 and humidity <= 60:
                    yield "high wind, medium humidity", (temperature, 1)
                  else:
                    yield "high wind, high humidity", (temperature, 1)
            
            except ValueError:
                pass

    def reducer(self, key, values):
        sum_values = 0
        count = 0
        for value in values:
            sum_values += value[0]
            count += value[1]
        if count > 0:
            avg = sum_values / count
            yield key, avg

if __name__ == '__main__':
    ForestFireAvarageByLevels.run()
