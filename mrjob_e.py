from mrjob.job import MRJob
from mrjob.step import MRStep

class TipsToRevenueRatioJob(MRJob):
    
    def mapper(self, _, line):
        # Split the line into columns
        columns = line.split(',')
        
        # Extract the pickup location ID, tip amount, and total amount columns
        pickup_location_id = columns[5]
        tip_amount = float(columns[-4])
        total_amount = float(columns[-2])
        
        # Calculate the tips to revenue ratio
        tips_to_revenue_ratio = tip_amount / total_amount if total_amount > 0 else 0
        
        # Emit the pickup location ID as the key and tips to revenue ratio as the value
        yield pickup_location_id, tips_to_revenue_ratio
    
    def reducer(self, pickup_location_id, ratios):
        # Calculate the average tips to revenue ratio for each pickup location
        ratio_sum = sum(ratios)
        ratio_count = len(ratios)
        average_ratio = ratio_sum / ratio_count if ratio_count > 0 else 0
        
        # Emit the pickup location ID and average tips to revenue ratio
        yield pickup_location_id, average_ratio
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer)
        ]

if __name__ == '__main__':
    TipsToRevenueRatioJob.run()
