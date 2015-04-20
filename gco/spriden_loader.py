#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Loads a dump of the SPRIDEN database from the University of Illinois
Banner system into the ORIA database.

Input data is expected to be comma-separated, with strings delimited
by an arbitrary punctuation character (as the data is quite dirty).

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"20 April 2015"
__version__ = 1.1

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

import csv
from MySQLdb import DateFromTicks
from time import mktime, strptime

class OracleDump(csv.excel):
    quotechar = '}'

class SpridenReadError( Exception ):
    """
    Raised when a CSV row is mal-formed.
    """
    pass

def parse_oracle_date( date_str ):
    """
    Turn an Oracle date string representation (dd-MON-yy) into a
    MySQL-compatible date object.
    """
    if date_str is None or date_str.strip() == '':
        return None

    return DateFromTicks( mktime( strptime( date_str, "%d-%b-%y" ) ) )

def main():
    """
    Read in a command-line specified CSV file and load it into the
    database.
    """

    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads CSV SPRIDEN data into ORIA DB"
    )
    parser.add_argument( "file", type=open,
                         help="CSV file(s) to read" )
    args = parser.parse_args()

    # Connect to the database.
    # Open a database connection (default to test DB).
    if args.db is None:
        args.db = oria.DB_BASE_TEST
    db = oria.DBConnection( db=args.db, offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # We’ll do this all in one transaction in case something goes
    # wrong.
    db.start()

    # Read in the dump file.
    csv_reader = csv.reader( args.file, dialect=OracleDump )
    for line_num, row in enumerate( csv_reader ):
        # Skip command rows.
        if len(row) <= 0 or row[0].startswith('SQL>'):
            continue

        if len(row) != 25:
            raise SpridenReadError, \
                "Line %d has %d columns!" % ( line_num, len(row) )

        if line_num % 100000 == 0:
            print( line_num )

        # Parse the row
        pidm, id, last, first, mi, change, entity, activity, user, \
            origin, search_last, search_first, search_mi, \
            soundex_last, soundex_first, ntyp, create_user, \
            create_date, data_origin, fdmn, prefix, surrogate, \
            version, user_id, vpdi = row

        # Fix up dates.
        row[7] = parse_oracle_date( row[7] ) # activity date
        row[17] = parse_oracle_date( row[17] )

        # Write the new data to the database!
        db.write( "INSERT INTO spriden_raw " +
                  "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                  "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                  "%s, %s, %s, %s );", # 25 columns
                  row )

    db.finish()

    return

if __name__ == '__main__':
    main()
    exit( 0 )
