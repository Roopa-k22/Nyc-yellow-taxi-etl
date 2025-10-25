import apache_beam as beam

class ParseCSVRow(beam.DoFn):
    def __init__(self, delimiter=","):
        self.delimiter = delimiter

    def process(self, element):
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(element), delimiter=self.delimiter)
        for row in reader:
            yield row
