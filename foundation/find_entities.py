#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Read a CSV with FACTS IDs, and add a column with our master IDs,
PIDMs, and/or EDW IDs.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"20 January 2015"
__version__ = 1.2

# Adjust the load path for common data loading operations.
import sys
from os import path
sys.path.append(
    path.join(
        path.dirname( path.dirname( path.abspath( __file__ ) ) ),
        'lib'
        )
    )

# Common data loading tools.
import oria

from argparse import FileType
import csv

def get_other_ids( db, facts_id ):
    """
    Given a FACTS ID, look up all corresponding IDs, and return a
    dictionary.  The keys of the dictionary are master, PIDM, Banner,
    and EDW; the values are dictionaries themselves, with keys being
    string IDs, and values all being True.
    """
    other_ids = { "master" : {} }

    other_id_rows = db.read_many(
        "SELECT m2.master_id, m2.other_id, s2.name " +
            "FROM master_external_org_other_id AS m1, " +
            "master_external_org_other_id AS m2, " +
            "master_other_id_scheme AS s1, " +
            "master_other_id_scheme AS s2 " +
            "WHERE s1.name = 'FACTS' " +
            "AND m1.scheme = s1.id " +
            "AND m1.other_id = %s " +
            "AND m2.master_id = m1.master_id " +
            "AND s2.id = m2.scheme;",
        ( facts_id, )
    )

    # Put the results into the dictionary.  We may have multiple
    # values for any ID scheme, including the master (though we hope
    # not).
    for other_id_row in other_id_rows:
        other_ids[ "master" ][ str( other_id_row[0] ) ] = True

        scheme = other_id_row[2]
        if scheme not in other_ids:
            other_ids[ scheme ] = {}
        other_ids[ scheme ][ other_id_row[1] ] = True

    return other_ids

def main():
    """
    Read in a command-line specified CSV file and write it back out
    with additions.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="add other IDs to FACTS IDs in a CSV"
    )
    parser.add_argument(
        "--outfile", "-o",
        type=FileType('w'),
        default=sys.stdout,
        help="file to write (STDOUT default)"
    )
    parser.add_argument(
        "--facts_header", "--fh",
        default="FactsID",
        help="CSV header label for FACTS ID"
    )
    parser.add_argument(
        "--banner",
        action="store_true",
        help="include Banner IDs in the output"
    )
    parser.add_argument(
        "--edw",
        action='store_true',
        help="include EDW IDs in the output"
    )
    parser.add_argument(
        "--pidm",
        action='store_true',
        help="include Banner PIDMs in the output"
    )
    parser.add_argument(
        "--nomaster",
        action='store_true',
        help="do not include entity master IDs in the output"
    )
    parser.add_argument(
        "--multirow",
        action='store_true',
        help="explode rows with multiple corresponding IDs"
    )
    parser.add_argument(
        "infile",
        type=open,
        help="CSV file to read"
    )
    args = parser.parse_args()

    # Open a connection to the ORIA database.
    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Open CSV interfaces on in and out files.
    csv_reader = csv.reader( args.infile )
    csv_writer = csv.writer( args.outfile )

    # The column number with the FACTS ID in it.
    facts_col = None

    # Read each row.
    for row in csv_reader:
        # Assume the first row is the header.  Find the FACTS ID
        # column; adjust the header appropriately, and emit it.
        if not facts_col:
            new_headers = []

            for i in range(len(row)):
                if row[i] == args.facts_header:
                    facts_col = i
                    break
            if not args.nomaster:
                new_headers.append( "Master ID" )
            if args.banner:
                new_headers.append( "Banner ID" )
            if args.pidm:
                new_headers.append( "PIDM" )
            if args.edw:
                new_headers.append( "EDW ID" )

            csv_writer.writerow(
                row[0:facts_col+1] + new_headers + row[facts_col+1:]
            )

            continue

        facts_id = row[facts_col]
        other_ids = get_other_ids( db, facts_id )

        # Let’s do some error checking.
        if "FACTS" in other_ids and \
           len( other_ids[ "FACTS" ] ) > 1:
            sys.stderr.write(
                "Entity has more than one FACTS ID:\n    " +
                "%s\n" % ", ".join( other_ids[ "FACTS" ].keys() )
            )

        # Set up the columns for output, regardless of multirow or
        # not.
        column_keys = []
        if not args.nomaster:
            column_keys.append( "master" )
        if args.banner:
            column_keys.append( "Banner" )
            if "Banner" not in other_ids:
                other_ids[ "Banner" ] = {}
        if args.pidm:
            column_keys.append( "PIDM" )
            if "PIDM" not in other_ids:
                other_ids[ "PIDM" ] = {}
        if args.edw:
            column_keys.append( "EDW" )
            if "EDW" not in other_ids:
                other_ids[ "EDW" ] = {}

        # If multirow, we explode a row, with one correlated ID per
        # column per row.
        if args.multirow:
            # Create a set of tuples for each row of output.  We’ll
            # output mostly-redundant rows for each variation of
            # correlated ID.

            # Here’s the clever bit: the * operator unwraps the list
            # of lists and makes them discrete arguments to map(),
            # giving us exactly the zipped set of tuples we need.
            output_ids = map(
                None,
                *[ other_ids[k].keys() for k in column_keys ]
            )

            # Ack, except map() gives us a list of strings if
            # column_keys has cardinality 1, and a list of tuples
            # otherwise.
            if len( column_keys ) == 1:
                output_ids = [ ( i, ) for i in output_ids ]

            # We need to write at least one row, even if offline or
            # something else when horribly awry.
            if not output_ids:
                csv_writer.writerow(
                    row[0:facts_col+1] + 
                    [ None for k in column_keys ] + 
                    row[facts_col+1:]
                )

            for ids in output_ids:
                csv_writer.writerow(
                    row[0:facts_col+1] +
                    list( ids ) + 
                    row[facts_col+1:]
                )

            continue

        # If not multirow, make a comma-separated list of IDs in
        # each column.
        csv_writer.writerow(
            row[0:facts_col+1] +
            [ ",".join( other_ids[k].keys() ) for k in column_keys ] +
            row[facts_col+1:]
        )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
