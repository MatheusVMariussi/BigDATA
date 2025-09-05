
from mrjob.job import MRJob

class ForestFireAvarageByLevelsV2(MRJob):
    def mapper(self, _, value):
        fields = value.split(",")
        if len(fields) == 13:
          try:
            temperature = float(fields[8])
            humidity = float(fields[9])
            wind = float(fields[10])

            if wind < 5:
              wind_level = "low wind"
            elif wind >= 5 and wind <= 10:
              wind_level = "medium wind"
            else:
              wind_level = "high wind"

            if humidity < 30:
              humidity_level = "low humidity"
            elif humidity >= 30 and humidity <= 60:
              humidity_level = "medium humidity"
            else:
              humidity_level = "high humidity"

            yield f"{wind_level}, {humidity_level}", (temperature, 1)

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
    ForestFireAvarageByLevelsV2.run()
