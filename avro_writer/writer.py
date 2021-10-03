"""Implements logic to read CSV file output into Avro file.
"""

from avro import schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from csv import DictReader
import os


def parse_schema(schema_path):
    """Parses Avro schema stored in JSON format

    :param schema_path: [description]
    :type schema_path: string
    :return: Parsed Avro schema of JSON input
    :rtype: Avro Schema object
    """
    return schema.parse(open(schema_path, "rb").read())


def avro_write(schema_path, csv_path, output_path):
    """Reads a CSV file from defined path and writes the output
    into an Avro file.

    :param schema_path: Path to Avro schema defined in JSON
    :type schema_path: string
    :param csv_path: Path to CSV file to write into Avro
    :type csv_path: string
    :param output_path: Path to write Avro file to including filename
    with avro extension
    :type output_path: string
    """
    avro_schema = parse_schema(schema_path)

    with DataFileWriter(
        open(output_path, "wb"),
        DatumWriter(),
        avro_schema
    ) as f_avro:
        with open(csv_path, "r") as f_csv:
            csv_reader = DictReader(f_csv)
            [f_avro.append(row) for row in csv_reader]


def main():
    """Main entry point for running Avro writer module."""
    resources = os.path.join(os.path.dirname(__file__), "..", "resources")
    avro_write(
        os.path.join(resources, "schema.json"),
        os.path.join(resources, "sample.csv"),
        os.path.join(resources, "..", "output.avro"),
    )
