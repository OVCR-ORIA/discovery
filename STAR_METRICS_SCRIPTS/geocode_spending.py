#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Given a detailed spending report, geocode the locations, and augment
the geocoding with Congressional districts.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"8 April 2015"
__version__ = 1.0

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

# I/O and API support.
from argparse import FileType
import csv
from geopy.geocoders.googlev3 import GoogleV3
import logging
from os import devnull
import sunlight # Sunlight Foundation Congressional API

# Constants.
CONGRESSIONAL_SOURCE_COMMENT = "geocode_spending.py v%0.1f" % __version__
CONGRESSIONAL_SOURCE_ID = 7
GEOCODING_SOURCE_COMMENT = "geocode_spending.py v%0.1f" % __version__
GEOCODING_SOURCE_ID = 6
SUNLIGHT_API_KEY = "dccfea47c6814ee5bdbdd2b954271282"
VENDOR_HEADER = [ "PeriodStartDate", "PeriodEndDate", "VendorPIDM",
                  "CFDACode", "SponsorID", "FundTypeCode",
                  "GrantTitle", "RecipientAccountNumber",
                  "VendorAddress1", "VendorAddress2", "VendorCity",
                  "VendorState", "VendorNation", "VendorZIP",
                  "VendorPaymentAmount" ]

class VendorInfoError( Exception ):
    """
    Raised if there is something wrong with the input file.
    """
    pass

def main():
    """
    Connect to the database, read the CSV file, update geocoding, and
    emit enhanced output.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description=
            "geocodes STAR METRICS spending data"
    )
    parser.add_argument( "--out", "-o", type=FileType('w'),
                         default=sys.stdout,
                         help=u"output file (STDOUT by default)" )
    parser.add_argument( "--logfile", "-l", default=sys.stderr,
                         type=FileType('w'),
                         help="log file (STDERR by default)" )
    parser.add_argument( "file", type=open, help="CSV file to read" )
    args = parser.parse_args()

    # Start logging.
    logfile = args.logfile
    logging.basicConfig(
        stream=logfile,
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logging.info( "Beginning vendor address geocoding." )

    # Start the reading and writing.
    reader = csv.reader( args.file )
    writer = csv.writer( args.out )

    # Check the input header
    header = reader.next()
    if header != VENDOR_HEADER:
        raise VendorInfoError, \
            u"Input header does not match expected format."

    # Open a database connection (default to test DB).
    if args.db is None:
        args.db = oria.DB_BASE_TEST
    db = oria.DBConnection( db=args.db, offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    logging.info( "DB connection open to %s." % args.db )

    # Instantiate our geocoder.
    gc = GoogleV3()

    # Alter the header appropriately and write it.
    header += [ "Latitude", "Longitude", "CDState", "CDNumber" ]
    writer.writerow( header )

    address_cache = {}

    # Handle each address-bearing row.
    for row in reader:
        # For each address, construct an un-normalized string.
        street1 = row[8].strip()
        street2 = row[9].strip()
        city = row[10].strip()
        state = row[11].strip()
        nation = row[12].strip()
        postcode = row[13].strip()
        addr_string = street1
        if street1 and ( street2 or city or state ):
            addr_string += ", "
        addr_string += street2
        if street2 and ( city or state ):
            addr_string += ", "
        addr_string += city
        if city and state:
            addr_string += ", "
        addr_string += state
        if postcode:
            addr_string += " "
        addr_string += postcode
        if nation:
            addr_string += " "
        addr_string += nation

        # Look for that string in the database.
        db.start()
        addr_id = lat = lon = addr_norm = None
        addr_row = db.read( "SELECT id, x(location) AS latitude, " +
                                "y(location) AS longitude " +
                                "FROM address " +
                                "WHERE addr_string = %s " +
                                "AND valid_end IS NULL;",
                            ( addr_string, ) )

        # If found, use the geocoding there.
        if addr_row is not None:
            addr_id = addr_row[0]
            lat, lon = addr_row[1:3]

        # If not, look up the geocode with geopy
        if lat is None or lon is None:
            logging.info( "Geocoding %s." % addr_string )
            loc = gc.geocode( addr_string )
            if loc is None:
                logging.warn( "Unable to find location for address %s"
                              % addr_string )
            else:
                lat, lon = loc.latitude, loc.longitude
                addr_norm = loc.address

                # Store the geocode and address
                if addr_id is None:
                    update_stmt = "INSERT INTO address " + \
                                  "( street1, street2, city, " + \
                                  "state_province, postcode, " + \
                                  "nation, addr_string, " + \
                                  "addr_string_norm, location, " + \
                                  "source, source_comment ) " + \
                                  "VALUES ( %s, %s, %s, %s, %s, " + \
                                  "%s, %s, POINT( %s, %s ), %s, " + \
                                  "%s, %s );"
                    params = ( street1, street2, city, state,
                               postcode, nation, addr_string,
                               addr_norm, lat, lon,
                               GEOCODING_SOURCE_ID,
                               GEOCODING_SOURCE_COMMENT )
                else:
                    update_stmt = "UPDATE address SET " + \
                                  "addr_string_norm = %s, " + \
                                  "location = POINT( %s, %s ), " + \
                                  "source = %s, " + \
                                  "source_comment = %s " + \
                                  "WHERE id = %s;"
                    params = ( addr_norm, lat, lon,
                               GEOCODING_SOURCE_ID,
                               GEOCODING_SOURCE_COMMENT )
                db.write( update_stmt, params )

        # Link the address with the PIDM.
        # Look for a congressional district (less than a year old).
        # If found, use it.
        # If not, look it up with sunlight; store and date the result.

        # Commit the changes we made for this row.
        db.finish()

        # Write out the result.
        writer.writerow( row )

        break

    return

if __name__ == '__main__':
    main()
    exit( 0 )
