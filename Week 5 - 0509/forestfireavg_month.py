
from mrjob.job import MRJob

class ForestFireAvarageTempMonth(MRJob):
    def mapper(self, _, value):
        fields = value.split(",")
        if len(fields) == 13:
            try:
                temperature = float(fields[8])
                month = fields[2]
                yield "AverageAll", (temperature, 1) # Key for overall average
                yield f"{month}", (temperature, 1) # Key for monthly average
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
    ForestFireAvarageTempMonth.run()
