# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta
from statistics import mean

class MostPickupRevenue(MRJob):

    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            row = line.strip().split(',')
            droptime = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
            pickuptime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            triptime = (droptime - pickuptime).seconds
            yield row[7], triptime

    def reducer(self, pu_id, trip_times):
        avg_trip_time = mean(trip_times)
        delta = timedelta(avg_trip_time)
        formatted_time = str(delta)
        yield pu_id, formatted_time


if __name__ == '__main__':
    MostPickupRevenue.run()

""" Command:
python mrtask_b.py input > out.txt
"""