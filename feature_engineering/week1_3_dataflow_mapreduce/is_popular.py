#!/usr/bin/env python

import apache_beam as beam
import argparse


def starts_with(line, term):
    if line.startswith(term):
        yield line


def split_package_name(package_name):
    """
    e.g. given com.example.appname.library.widgetname
    returns
        com
        com.example
        com.example.appname
    etc.
    """
    result = []
    end = package_name.find('.')
    while end > 0:
        result.append(package_name[0:end])
        end = package_name.find('.', end + 1)
    result.append(package_name)
    return result


def get_packages(line, keyword):
    start = line.find(keyword) + len(keyword)
    end = line.find(';', start)
    if start < end:
        package_name = line[start:end].strip()
        return split_package_name(package_name)
    return []


def package_use(line, keyword):
    packages = get_packages(line, keyword)
    for p in packages:
        yield (p, 1)


def by_value(kv1, kv2):
    key1, value1 = kv1
    key2, value2 = kv2
    return value1 < value2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Find the most used Java packages')
    parser.add_argument(
        '--output_prefix',
        default='tmp/output',
        help='Output prefix',
    )
    parser.add_argument(
        '--input',
        default='javahelp/*.java',
        help='Input directory',
    )

    options, pipeline_args = parser.parse_known_args()
    p = beam.Pipeline(argv=pipeline_args)

    input_path = '{0}'.format(options.input)
    output_prefix = options.output_prefix
    keyword = 'import'

    # find most used packages
    (p
        | 'GetJava' >> beam.io.ReadFromText(input_path)
        | 'GetImports' >> beam.FlatMap(lambda line: starts_with(line, keyword))
        | 'PackageUse' >> beam.FlatMap(lambda line: package_use(line, keyword))
        | 'TotalUse' >> beam.CombinePerKey(sum)
        | 'Top_5' >> beam.transforms.combiners.Top.Of(5, by_value)
        | 'write' >> beam.io.WriteToText(output_prefix)
     )

    p.run().wait_until_finish()
