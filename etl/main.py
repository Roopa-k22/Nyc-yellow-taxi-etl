import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from etl.transforms import ParseCSVRow

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--error_table', required=True)
    parser.add_argument('--schema', required=True)       # main table schema (GCS path)
    parser.add_argument('--error_schema', required=True) # error table schema (GCS path)
    parser.add_argument('--delimiter', default=',')
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--runner', default='DataflowRunner')
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        # Read CSV lines
        lines = p | "Read CSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
        
        # Parse rows, returns a tuple: (valid_row_dict, error_row_dict)
        parsed_results = lines | "Parse CSV" >> beam.ParDo(ParseCSVRow(args.delimiter)).with_outputs('errors', main='valid')

        valid_rows = parsed_results.valid
        error_rows = parsed_results.errors

        # Write valid rows to main BigQuery table
        valid_rows | "Write Valid Rows" >> beam.io.WriteToBigQuery(
            table=args.output,
            schema=args.schema,  # pass GCS path directly
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Write error rows to error BigQuery table
        error_rows | "Write Error Rows" >> beam.io.WriteToBigQuery(
            table=args.error_table,
            schema=args.error_schema,  # pass GCS path directly
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == "__main__":
    run()
