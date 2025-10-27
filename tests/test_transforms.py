import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline

class ParseCSVRow(beam.DoFn):
    def __init__(self, delimiter="\t"):
        self.delimiter = delimiter

    def process(self, element):
        # Skip header row
        if element.startswith("VendorID"):
            return

        values = element.split(self.delimiter)

        row = {
            'VendorID': values[0],
            'tpep_pickup_datetime': values[1],
            'tpep_dropoff_datetime': values[2],
            'passenger_count': int(values[3]) if values[3] else 0,
            'trip_distance': float(values[4]) if values[4] else 0.0,
            'pickup_longitude': float(values[5]) if values[5] else None,
            'pickup_latitude': float(values[6]) if values[6] else None,
            'RatecodeID': values[7],
            'store_and_fwd_flag': values[8],
            'dropoff_longitude': float(values[9]) if values[9] else None,
            'dropoff_latitude': float(values[10]) if values[10] else None,
            'payment_type': values[11],
            'fare_amount': float(values[12]) if values[12] else None,
            'extra': float(values[13]) if values[13] else None,
            'mta_tax': float(values[14]) if values[14] else None,
            'tip_amount': float(values[15]) if values[15] else None,
            'tolls_amount': float(values[16]) if values[16] else None,
            'improvement_surcharge': float(values[17]) if values[17] else None,
            'total_amount': float(values[18]) if values[18] else None
        }

        yield row


def test_parse_csv():
    input_data = [
        "VendorID\ttpep_pickup_datetime\ttpep_dropoff_datetime\tpassenger_count\ttrip_distance\tpickup_longitude\tpickup_latitude\tRatecodeID\tstore_and_fwd_flag\tdropoff_longitude\tdropoff_latitude\tpayment_type\tfare_amount\textra\tmta_tax\ttip_amount\ttolls_amount\timprovement_surcharge\ttotal_amount",
        "2\t1/1/16 0:00\t1/1/16 0:00\t2\t1.1\t-73.9903717\t40.734695434570313\t1\tN\t-73.98184204\t40.732406616210937\t2\t7.5\t0.5\t0.5\t0\t0\t0.3\t8.8"
    ]

    expected_valid = [{
        'VendorID': '2',
        'tpep_pickup_datetime': '1/1/16 0:00',
        'tpep_dropoff_datetime': '1/1/16 0:00',
        'passenger_count': 2,
        'trip_distance': 1.1,
        'pickup_longitude': -73.9903717,
        'pickup_latitude': 40.734695434570313,
        'RatecodeID': '1',
        'store_and_fwd_flag': 'N',
        'dropoff_longitude': -73.98184204,
        'dropoff_latitude': 40.732406616210937,
        'payment_type': '2',
        'fare_amount': 7.5,
        'extra': 0.5,
        'mta_tax': 0.5,
        'tip_amount': 0.0,
        'tolls_amount': 0.0,
        'improvement_surcharge': 0.3,
        'total_amount': 8.8
    }]

    with TestPipeline() as p:
        results = (
            p
            | beam.Create(input_data)
            | beam.ParDo(ParseCSVRow(delimiter="\t")).with_outputs('errors', main='valid')
        )

        valid_rows = results.valid
        error_rows = results.errors

        assert_that(valid_rows, equal_to(expected_valid))
        assert_that(error_rows, equal_to([]), label='CheckEmptyErrors')
