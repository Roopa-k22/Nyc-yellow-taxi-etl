from transforms import ParseCSVRow
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

def test_parse_csv():
    input_data = ["vendor_id,trip_distance\n1,2.5"]
    expected = [{"vendor_id": "1", "trip_distance": "2.5"}]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input_data)
            | beam.ParDo(ParseCSVRow(delimiter=","))
        )
        assert_that(output, equal_to(expected))
