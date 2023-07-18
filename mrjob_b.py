# Which pickup location generates the most revenue?

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPickupRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            row = line.strip().split(',')
            yield row[7], float(row[16])

    def reducer(self, pu_id, rev):
        total_rev = sum(rev)
        yield None, (total_rev, pu_id)

    def final_reducer(self, _, values):
        max_revenue, pu_id = max(values)
        yield pu_id, max_revenue


if __name__ == '__main__':
    MostPickupRevenue.run()

""" Command:
python mrtask_b.py input > out.txt
"""