import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from transforms import ParseCSVRow
from schemas import load_schema_from_gcs

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--error_table', required=True)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--delimiter', default=',')
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--runner', default='DataflowRunner')
    args, pipeline_args = parser.parse_known_args(argv)

    schema = load_schema_from_gcs(args.schema)

    options = PipelineOptions(
        pipeline_args,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        lines = p | "Read CSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
        parsed = lines | "Parse CSV" >> beam.ParDo(ParseCSVRow(args.delimiter))

        parsed | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            args.output,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == "__main__":
    run()
