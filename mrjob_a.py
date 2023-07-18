from mrjob.job import MRJob
from mrjob.step import MRStep

class TripsRevenueJob(MRJob):
    
    def mapper(self, _, line):
        # Split the line into columns
        columns = line.split(',')
        
        # Extract the vendor ID and total amount columns
        vendor_id = columns[0]
        total_amount = float(columns[-2])
        
        # Emit the vendor ID as the key and total amount as the value
        yield vendor_id, total_amount
    
    def reducer(self, vendor_id, amounts):
        # Calculate the total revenue for each vendor
        total_revenue = sum(amounts)
        
        # Emit the vendor ID and total revenue
        yield vendor_id, total_revenue
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    TripsRevenueJob.run()
