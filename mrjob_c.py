from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypesJob(MRJob):
    
    def mapper(self, _, line):
        # Split the line into columns
        columns = line.split(',')
        
        # Extract the payment type column
        payment_type = columns[-3]
        
        # Emit the payment type as the key and count as 1
        yield payment_type, 1
    
    def reducer(self, payment_type, counts):
        # Calculate the total count for each payment type
        total_count = sum(counts)
        
        # Emit the payment type and total count
        yield payment_type, total_count
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer)
        ]

if __name__ == '__main__':
    PaymentTypesJob.run()
