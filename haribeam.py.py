import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'haridemoproject-339510'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://hari_bucket9966/*.CSV', skip_header_lines =1)
       |  "parse" >> beam.Map(CSV.loads)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:haridataset.final'.format(PROJECT_ID),
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()