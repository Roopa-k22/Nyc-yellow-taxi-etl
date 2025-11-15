import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from datetime import datetime
import os
import pyarrow as pa
import logging

# --------------------------
# Transform Function
# --------------------------
class TransformRow(beam.DoFn):
    def __init__(self):
        # Counter for debugging skipped rows
        self.bad_rows = beam.metrics.Metrics.counter(self.__class__, "bad_rows")
        self.good_rows = beam.metrics.Metrics.counter(self.__class__, "good_rows")

    def process(self, row):
        values = next(csv.reader([row]))

        def safe_int(x):
            try:
                return int(x)
            except:
                return None

        def safe_float(x):
            try:
                return float(x)
            except:
                return None

        try:
            record = {
                'VendorID': safe_int(values[0]),
                'tpep_pickup_datetime': values[1],
                'tpep_dropoff_datetime': values[2],
                'passenger_count': safe_int(values[3]),
                'trip_distance': safe_float(values[4]),
                'RatecodeID': safe_int(values[5]),
                'store_and_fwd_flag': values[6],
                'pickup_longitude': safe_float(values[7]),
                'pickup_latitude': safe_float(values[8]),
                'dropoff_longitude': safe_float(values[9]),
                'dropoff_latitude': safe_float(values[10]),
                'payment_type': safe_int(values[11]),
                'fare_amount': safe_float(values[12]),
                'extra': safe_float(values[13]),
                'mta_tax': safe_float(values[14]),
                'tip_amount': safe_float(values[15]),
                'tolls_amount': safe_float(values[16]),
                'improvement_surcharge': safe_float(values[17]),
                'total_amount': safe_float(values[18]),
            }
            self.good_rows.inc()
            yield record

        except Exception as e:
            logging.warning(f"Skipping bad row: {row} | Error: {e}")
            self.bad_rows.inc()


# --------------------------
# Main Pipeline
# --------------------------
def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=False, default='gs://etl-nyctaxi-bucket/nyc_taxi/*.csv')
    parser.add_argument('--parquet_output', required=False, default='gs://etl-nyctaxi-bucket/output/nyc_taxi_parquet/')
    parser.add_argument('--output_table', required=False, default='etl-demo-pipeline:nyctaxi_dataset.yellow_tripdata_all')
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True)

    # Define Parquet schema
    parquet_schema = pa.schema([
        ('VendorID', pa.int64()),
        ('tpep_pickup_datetime', pa.string()),
        ('tpep_dropoff_datetime', pa.string()),
        ('passenger_count', pa.int64()),
        ('trip_distance', pa.float64()),
        ('RatecodeID', pa.int64()),
        ('store_and_fwd_flag', pa.string()),
        ('pickup_longitude', pa.float64()),
        ('pickup_latitude', pa.float64()),
        ('dropoff_longitude', pa.float64()),
        ('dropoff_latitude', pa.float64()),
        ('payment_type', pa.int64()),
        ('fare_amount', pa.float64()),
        ('extra', pa.float64()),
        ('mta_tax', pa.float64()),
        ('tip_amount', pa.float64()),
        ('tolls_amount', pa.float64()),
        ('improvement_surcharge', pa.float64()),
        ('total_amount', pa.float64()),
    ])

    with beam.Pipeline(options=options) as p:
        # Step 1: Read and transform
        transformed = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | 'Transform' >> beam.ParDo(TransformRow())
        )

        #Optional: Print sample output for debugging (disable in production)
        transformed | 'DebugPrint' >> beam.Map(print)

        # Step 2: Write to Parquet
        (
            transformed
            | 'WriteParquet' >> beam.io.WriteToParquet(
                file_path_prefix=os.path.join(args.parquet_output, 'nyc_taxi'),
                schema=parquet_schema,
                file_name_suffix='.parquet',
                codec='SNAPPY'
            )
        )

        # Step 3: Write to BigQuery
        (
            transformed
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                args.output_table,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
