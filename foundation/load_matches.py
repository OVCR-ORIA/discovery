#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Loads a set of entity matches into the oria master DB.

Written for the University of Illinois
"""

__author__ = u"Boris Capitanu <capitanu@illinois.edu>"
__date__ = u"19 February 2015"
__version__ = 1.0

# Adjust the load path for common data loading operations
import sys
sys.path.append('../lib')

import argparse
import csv
import oria
from master import *

def str2auto_bool(arg):
    """
    Converts a string to a boolean value.

    Args:
        arg: The string to convert

    Returns:
        bool or None: The boolean value corresponding to the string, or
                      None if 'auto'

    Raises:
        ValueError: if specified string is not valid
    """
    state = arg.lower()
    if state in ("true", "yes", "1"):
        return True
    elif state in ("false", "no", "0"):
        return False
    elif state in ("auto"):
        return None
    else:
        raise ValueError(arg)

def main():
    """
    Read in a command-line specified CSV file and load the set of matches
    into the oria master DB
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="load entity matches into oria master DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.register('type', 'auto_bool', str2auto_bool)
    parser.add_argument(
        "--header",
        type="auto_bool",
        metavar="true|yes|false|no|auto",
        default="auto",
        help="Specifies whether the CSV contains a header"
    )
    parser.add_argument(
        "--scheme",
        metavar="NAME",
        required=True,
        help="Specifies the scheme name for the matched entities."
    )
    parser.add_argument(
        "--source",
        metavar="NAME",
        required=True,
        help="Specifies the data source name for matched entities."
    )
    parser.add_argument(
        "-c", "--comment",
        help="Specifies a comment to be associated with the matched entities."
    )
    parser.add_argument(
        "csvfile",
        type=open,
        help="CSV file to read"
    )
    args = parser.parse_args()

    # Open a connection to the ORIA database
    db = oria.DBConnection(
        offline=args.offline,
        db_write=args.db_write,
        debug=args.debug
    )

    # Get the scheme_id for the given scheme name
    try:
        scheme_id = get_scheme_id(db, args.scheme)
    except SchemeNonExistent:
        print "ERR: Unknown scheme: %s" % args.scheme
        # Display the list of supported schemes
        supported_schemes = get_supported_schemes(db)
        print "Supported schemes: %s" % supported_schemes
        exit(1)

    if args.debug:
        print "*** Using scheme: %s (id: %s)" % (args.scheme, scheme_id)

    # Get the source_id for the given data source name
    try:
        source_id = get_data_source_id(db, args.source)
    except DataSourceNonExistent:
        print "ERR: Unknown data source: %s" % args.source
        # Display the list of supported data sources
        supported_data_sources = get_supported_data_sources(db)
        print "Supported data sources: %s" % supported_data_sources
        exit(2)

    if args.debug:
        print "*** Using data source: %s (id: %s)" % (args.source, source_id)

    if args.header is not None:
        has_header = args.header
    else:
        # Detect whether a header is present in the CSV
        has_header = csv.Sniffer().has_header(args.csvfile.read(1024))
        if args.debug and has_header: print "*** Detected CSV header..."
        args.csvfile.seek(0)

    # Open input CSV
    csv_reader = csv.reader(args.csvfile)

    # Skip header if it exists
    if has_header:
        if args.debug: print "*** Skipping CSV header..."
        next(csv_reader)

    total = 0
    failed = 0

    # Read each row.
    for row in csv_reader:
        if len(row) < 2:
            print "WARN: Skipping invalid row: %s" % row
            continue

        other_id    = str(row[0])   # other id
        meo_id      = str(row[1])   # id from master_external_org
        other_name  = str(row[2])   # other name
        meo_name    = str(row[3])   # name from master_external_org

        try:
            # Assert a match between the entities with the given ids
            add_external_org_other_id(
                db, meo_id, other_id, scheme_id, source_id, args.comment
            )

            # Add the other_name as an alias for the organization in our master
            add_external_org_alias(
                db, meo_id, other_name, source_id, comment=args.comment
            )
        except Exception as e:
            print ("ERR: Could not assert match: other(%s) -> org(%s)!\n" + \
                  "Reason: %s") % (other_id, meo_id, e)
            failed += 1
        finally:
            total += 1

    print "All done. [total: {:0,d}, success: {:1,d}, failed: {:2,d}]".format(
        total, total-failed, failed
    )
    return

if __name__ == '__main__':
    main()
    exit(0)
