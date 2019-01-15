#!/usr/bin/env python

import apache_beam as beam
import sys


def my_grep(line, term):
    if line.startswith(term):
        yield line


if __name__ == '__main__':
    p = beam.Pipeline(argv=sys.argv)
    input_path = 'week2/javahelp/*.java'
    output_prefix = 'tmp/output'
    searchTerm = 'import'

    # find all lines that contain the searchTerm
    (p
        | 'GetJava' >> beam.io.ReadFromText(input_path)
        | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm))
        | 'write' >> beam.io.WriteToText(output_prefix)
     )

    p.run().wait_until_finish()
