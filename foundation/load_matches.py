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

import csv
import oria
from master import *

def main():
    """
    Read in a command-line specified CSV file and load the set of matches
    into the oria master DB
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="load entity matches into oria master DB"
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
        "infile",
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

    # Detect whether a header is present in the CSV
    has_header = csv.Sniffer().has_header(args.infile.read(1024))
    args.infile.seek(0)

    # Open input CSV
    csv_reader = csv.reader(args.infile)

    # Skip header if it exists
    if has_header:
        if args.debug: print "*** Detected CSV header, skipping..."
        next(csv_reader)

    total = 0
    failed = 0

    # Read each row.
    for row in csv_reader:
        if len(row) < 2:
            print "WARN: Skipping invalid row: %s" % row
            continue

        other_id = str(row[0])     # other id
        meo_id = str(row[1])       # id from master_external_org

        try:
            # Assert a match between the entities with the given ids
            add_external_org_other_id(
                db, meo_id, other_id, scheme_id, source_id, args.comment
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
