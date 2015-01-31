#!/usr/bin/env python
"""Basic ETL script for Cassandra"""

import argparse
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import csv


# Connect to the local Cassandra instance and use the 'accounts' keyspace
CLUSTER = Cluster(['127.0.0.1'])
SESSION = CLUSTER.connect()
SESSION.execute('USE accounts')

# Define a CQL prepared statement
INSERT_USERS = SESSION.prepare("""
    INSERT INTO users (
    id,
    fname,
    lname,
    email,
    group)
    VALUES (?,?,?,?,?)
""")

INSERT_USERS.consistency_level = ConsistencyLevel.ONE


def getopts():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=argparse.FileType('r'),
                        required=True, help="input file (.csv)")
    return parser.parse_args()


def extract(input):
    """Read the CSV file"""
    reader = csv.DictReader(input)
    return reader


def transform(input):
    """Split name into fname and lname"""
    transformed_file = []

    for row in input:
        names = row['name'].split()
        row['fname'] = names[0]
        row['lname'] = names[1]
        del row['name']
        transformed_file.append(row)
    return transformed_file


def load(input):
    """INSERT data into Cassandra table"""
    for row in input:
        SESSION.execute(INSERT_USERS,
                        [int(row['id']),
                         row['fname'],
                         row['lname'],
                         row['email'],
                         row['group']])


if __name__ == '__main__':
    CLI_ARGS = getopts()

    # Extract
    input_data = extract(CLI_ARGS.input)

    # Transform
    transformed_data = transform(input_data)

    # Load
    load(transformed_data)

    print "ETL script complete."
