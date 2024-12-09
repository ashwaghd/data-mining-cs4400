from mrjob.job import MRJob
import csv

class StatsJob(MRJob):

    def mapper(self, _, line):
        fields = next(csv.reader([line]))  # Parse line as CSV, get the first (and only) list element
        try:
            if fields[10] != "Message" and fields[10] != '':
                value = float(fields[10])  # Access the "Data Value" field by index
                yield "stats", (value, 1)
        except (IndexError, ValueError):
            # Handle rows that might not have enough fields or have invalid data
            pass

    def reducer(self, key, values):
        min_value = float('inf')
        max_value = float('-inf')
        total = 0
        count = 0

        for value, cnt in values:
            if value is not None:
                min_value = min(min_value, value)
                max_value = max(max_value, value)
                total += value
                count += cnt

        # Yield results after the loop
        yield "min", min_value
        yield "max", max_value
        yield "count", count
        yield "average", total / count if count > 0 else None

if __name__ == '__main__':
    StatsJob.run()

