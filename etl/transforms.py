import apache_beam as beam
import csv
from io import StringIO

class ParseCSVRow(beam.DoFn):
    """
    Parse CSV rows and separate valid rows from error rows.
    Emits valid rows to main output and invalid rows to 'errors' side output.
    """
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, element):
        try:
            # Use csv.reader to handle quoting and delimiters
            reader = csv.DictReader(StringIO(element), delimiter=self.delimiter, fieldnames=[
                'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID',
                'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                'congestion_surcharge', 'airport_fee'
            ])
            row = next(reader)

            # Optional: simple validation example
            row['passenger_count'] = int(row['passenger_count']) if row['passenger_count'] else 0
            row['trip_distance'] = float(row['trip_distance']) if row['trip_distance'] else 0.0

            yield row  # main output (valid row)

        except Exception as e:
            # send invalid rows to side output
            yield beam.pvalue.TaggedOutput('errors', {
                'raw_row': element,
                'error': str(e)
            })
