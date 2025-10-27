from etl.transforms import ParseCSVRow
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam import pvalue

def test_parse_csv():
    input_data = ["VendorID,tpep_pickup_datetime\n1,2025-10-26T12:00:00"]
    
    # Expected valid row with default/None for missing columns
    expected_valid = [{
        'VendorID': '1',
        'tpep_pickup_datetime': '2025-10-26T12:00:00',
        'tpep_dropoff_datetime': None,
        'passenger_count': 0,
        'trip_distance': 0.0,
        'RatecodeID': None,
        'store_and_fwd_flag': None,
        'PULocationID': None,
        'DOLocationID': None,
        'payment_type': None,
        'fare_amount': None,
        'extra': None,
        'mta_tax': None,
        'tip_amount': None,
        'tolls_amount': None,
        'improvement_surcharge': None,
        'total_amount': None,
        'congestion_surcharge': None,
        'airport_fee': None
    }]

    with TestPipeline() as p:
        results = (
            p
            | beam.Create(input_data)
            | beam.ParDo(ParseCSVRow(delimiter=",")).with_outputs('errors', main='valid')
        )

        valid_rows = results.valid
        error_rows = results.errors

        # Assert valid rows match expected
        assert_that(valid_rows, equal_to(expected_valid))

        # Assert no error rows
        assert_that(error_rows, equal_to([]), label='CheckEmptyErrors')
