
from mrjob.job import MRJob

class ForestFireAvarageTempComplex(MRJob):
    def mapper(self, _, value):
        fields = value.split(",")
        if len(fields) == 13:
            try:
                temperature = float(fields[8])
                month = fields[2]
                day = fields[3]
                wind = float(fields[10])
                if wind > 2:
                  yield f"{month}, {day}", (temperature, 1)
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
    ForestFireAvarageTempComplex.run()
