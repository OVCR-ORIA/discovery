#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Given a detailed spending report, geocode the locations, and augment
the geocoding with Congressional districts.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"16 April 2015"
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
import master
import oria

# I/O and API support.
from argparse import FileType
import csv
from geopy.geocoders.googlev3 import GoogleV3
import logging
from os import devnull
import re
import sunlight # Sunlight Foundation Congressional API

# Constants.
BANNER_SOURCE_COMMENT = "STAR METRICS vendor detail report"
BANNER_SOURCE_ID = 1
CONGRESSIONAL_SOURCE_COMMENT = "geocode_spending.py v%0.1f" % __version__
CONGRESSIONAL_SOURCE_ID = 7
GEOCODING_SOURCE_COMMENT = "geocode_spending.py v%0.1f" % __version__
GEOCODING_SOURCE_ID = 6
PIDM_ID = 1
SUNLIGHT_API_KEY = "dccfea47c6814ee5bdbdd2b954271282"
VENDOR_HEADER = [ "PeriodStartDate", "PeriodEndDate", "VendorPIDM",
                  "CFDACode", "SponsorID", "FundTypeCode",
                  "GrantTitle", "RecipientAccountNumber",
                  "VendorAddress1", "VendorAddress2", "VendorCity",
                  "VendorState", "VendorNation", "VendorZIP",
                  "VendorPaymentAmount" ]
ZIP_PLUS_RE = r'^([0-9]{5})-[0-9]{4}'

# Compile-once regexps (not actually constants but hey).
zip_plus_re = re.compile( ZIP_PLUS_RE )

# Query constants.
QUERY_INSERT_ADDR_CD = "INSERT IGNORE INTO " + \
                       "address_congressional_district " + \
                       "( address, congressional_district, " + \
                       "source, source_comment ) " + \
                       "VALUES ( %s, %s, %s, %s );"
QUERY_INSERT_ADDR_GEOCODE = "INSERT INTO address " + \
                            "( street1, street2, city, " + \
                            "state_province, postcode, nation, " + \
                            "addr_string, addr_string_norm, " + \
                            "state_province_ref, postcode_ref, " + \
                            "nation_ref, location, source, " + \
                            "source_comment ) " + \
                            "VALUES ( %s, %s, %s, %s, %s, %s, " + \
                            "%s, %s, %s, %s, %s, " + \
                            "POINT( %s, %s ), %s, %s );"
QUERY_INSERT_CD = "INSERT IGNORE INTO congressional_district " + \
                  "( state, district_number, source, " + \
                  "source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s );"
QUERY_INSERT_ORG_ADDR = "INSERT IGNORE INTO " + \
                        "master_external_org_address " + \
                        "( external_org, address, source, " + \
                        "source_comment ) VALUES ( %s, %s, %s, %s );"
QUERY_SELECT_ADDR_CD = "SELECT cd.id, st.code, " + \
                       "cd.district_number " + \
                       "FROM address_congressional_district " + \
                       "AS acd, " + \
                       "congressional_district AS cd, " + \
                       "country_div_1 AS st " + \
                       "WHERE acd.address = %s " + \
                       "AND acd.valid_end IS NULL " + \
                       "AND acd.valid_start >= " + \
                       "(NOW() - INTERVAL 1 YEAR) " + \
                       "AND cd.id = acd.congressional_district " + \
                       "AND st.id = cd.state;"
QUERY_SELECT_CD = "SELECT cd.id " + \
                  "FROM congressional_district AS cd, " + \
                  "country_div_1 AS st " + \
                  "WHERE st.code = %s AND cd.state = st.id " + \
                  "AND cd.district_number = %s " + \
                  "AND cd.valid_end IS NULL;"
QUERY_SELECT_LOCATION = "SELECT a.id, c.iso3166, " + \
                        "x(a.location) AS latitude, " + \
                        "y(a.location) AS longitude " + \
                        "FROM address AS a, country AS c " + \
                        "WHERE a.addr_string = %s " + \
                        "AND c.id = a.nation_ref " + \
                        "AND a.valid_end IS NULL;"
QUERY_SELECT_STATE_PROV = "SELECT id FROM country_div_1 " + \
                          "WHERE country = %s AND code = %s;"
QUERY_SELECT_ZIP = "SELECT z.id FROM postcode AS z, " + \
                   "country_div_1 AS s " + \
                   "WHERE z.postcode = %s AND s.id = z.div_1 " + \
                   "AND s.country = %s;"
QUERY_UPDATE_ADDR_GEOCODE = "UPDATE address " + \
                            "SET addr_string_norm = %s, " + \
                            "state_province_ref = %s, " + \
                            "postcode_ref = %s, " + \
                            "nation_ref = %s, " + \
                            "location = POINT( %s, %s ), " + \
                            "source = %s, " + \
                            "source_comment = %s " + \
                            "WHERE id = %s;"

class VendorInfoError( Exception ):
    """
    Raised if there is something wrong with the input file.
    """
    pass

def add_org_to_addr( db, addr_id, org_id ):
    """
    Add an association between an external organization and an address
    in the database.
    """
    db.write( QUERY_INSERT_ORG_ADDR,
              ( org_id, addr_id, BANNER_SOURCE_ID,
                BANNER_SOURCE_COMMENT ) )
    return

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

    # Configure the sunlight tool.
    sunlight.config.API_KEY = SUNLIGHT_API_KEY

    # Alter the header appropriately and write it.
    header += [ "Latitude", "Longitude", "CDState", "CDNumber" ]
    writer.writerow( header )

    address_cache = {}
    cd_cache = {}
    nation_cache = {}
    pidm_cache = {}
    state_prov_cache = { "US" : {} }
    zip_cache = {}

    rowcount = 0

    # Handle each address-bearing row.
    for row in reader:
        # Find the organization in question.
        pidm = row[2].strip()
        if pidm in pidm_cache:
            org_id = pidm_cache[ pidm ]
        else:
            org_id = master.get_master_id_for_other_id( db, pidm,
                                                        PIDM_ID )
            pidm_cache[ pidm ] = org_id

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

        # See if we already handled this string this time through.
        if addr_string in address_cache:
            addr_id, lat, lon, nation_code, cd_st, cd_num, orgs = \
                address_cache[ addr_string ]
            if org_id not in orgs:
                db.start()
                add_org_to_addr( db, addr_id, org_id )
                orgs.append( org_id )
                db.finish()
            row += [ lat, lon, cd_st, cd_num ]
            writer.writerow( row )
            continue

        # Look for that string in the database.
        db.start()
        addr_id = lat = lon = addr_norm = nation_code = None
        addr_row = db.read( QUERY_SELECT_LOCATION,
                            ( addr_string, ) )

        # If found, use the geocoding there.
        if addr_row is not None:
            addr_id, nation_code, lat, lon = addr_row

        # If not, look up the geocode with geopy
        if lat is None or lon is None:
            logging.info( "Geocoding %s." % addr_string )
            loc = gc.geocode( addr_string )
            if loc is None:
                logging.warn( "Unable to find location for address %s"
                              % addr_string )
            else:
                # Get the latitude and longitude and normalized
                # address string from the response.
                lat, lon = loc.latitude, loc.longitude
                addr_norm = loc.address

                # Also parse the raw response for structured address
                # information.
                addr_struct = loc.raw[ "address_components" ]
                st_prov_code = st_prov_id = None
                postcode_code = postcode_id = None
                nation_id = None
                for component in addr_struct:
                    if "administrative_area_level_1" in \
                       component[ "types" ]:
                        st_prov_code = component[ "short_name" ]
                    elif "postal_code" in component[ "types" ]:
                        postcode_code = component[ "short_name" ]
                    elif "country" in component[ "types" ]:
                        nation_code = component[ "short_name" ]

                # Find database IDs for the nation and state or
                # province.
                if nation_code is not None:
                    nation_id = db.fetch_id( "country", "iso3166",
                                             nation_code,
                                             cache=nation_cache )
                if nation_id is not None and st_prov_code is not None:
                    if nation_code in state_prov_cache and \
                       st_prov_code in \
                       state_prov_cache[ nation_code ]:
                        st_prov_id = state_prov_cache[ nation_code ]\
                                     [ st_prov_code ]
                    if st_prov_id is None:
                        st_prov_row = db.read( QUERY_SELECT_STATE_PROV,
                                               ( nation_id,
                                                 st_prov_code ) )
                        if st_prov_row is not None:
                            st_prov_id = st_prov_row[0]
                            if nation_code not in state_prov_cache:
                                state_prov_cache[ nation_code ] = {}
                            state_prov_cache[ nation_code ]\
                                [ st_prov_code ] = st_prov_id

                # If US, turn ZIP+4 and ZIP+9 into ZIP
                if nation_code == "US" and postcode_code is not None:
                    m = zip_plus_re.match( postcode_code )
                    if m is not None:
                        postcode_code = m.group(1)
                    if postcode_code in zip_cache:
                        postcode_id = zip_cache[ postcode_code ]
                    else:
                        zip_row = db.read( QUERY_SELECT_ZIP,
                                           ( postcode_code,
                                             nation_id ) )
                        if zip_row is not None:
                            postcode_id = zip_row[0]

                # Store the geocode and address
                if addr_id is None:
                    db.write( QUERY_INSERT_ADDR_GEOCODE,
                              ( street1, street2, city, state,
                                postcode, nation, addr_string,
                                addr_norm, st_prov_id, postcode_id,
                                nation_id, lat, lon,
                                GEOCODING_SOURCE_ID,
                                GEOCODING_SOURCE_COMMENT ) )
                    addr_id = db.get_last_id()
                else:
                    db.write( QUERY_UPDATE_ADDR_GEOCODE,
                              ( addr_norm, st_prov_id, postcode_id,
                                nation_id, lat, lon,
                                GEOCODING_SOURCE_ID,
                                GEOCODING_SOURCE_COMMENT,
                                addr_id ) )

        # Link the address with the organization, unless for some
        # reason we don’t know about the organization.
        if org_id is not None:
            add_org_to_addr( db, addr_id, org_id )

        # If the address is in the United States, look for a
        # congressional district (less than a year old).
        if nation_code == "US":
            district_row = db.read( QUERY_SELECT_ADDR_CD, ( addr_id, ) )
            if district_row is not None:
                cd_id, cd_st, cd_num = district_row[0:3]
            else:
                # If we didn’t find it, look it up with sunlight.
                logging.info( "Getting CD for %s (%f, %f)." %
                              ( addr_string, lat, lon) )
                district = sunlight.congress.\
                           locate_districts_by_lat_lon( lat, lon )
                cd_id = None
                cd_st = district[0][ "state" ]
                cd_num = district[0][ "district" ]

                # Store the results.

                # Find the district... in our cache?
                cd_code = "%s-%d" % ( cd_st, cd_num )
                if cd_code in cd_cache:
                    cd_id = cd_cache[ cd_code ]

                # ... in the database?
                if cd_id is None:
                    cd_row = db.read( QUERY_SELECT_CD, ( cd_st, cd_num ) )
                    if cd_row is not None:
                        cd_id = cd_row[0]

                # ... or make it new?
                if cd_id is None:
                    cd_st_id = None
                    if cd_st in state_prov_cache[ "US" ]:
                        cd_st_id = state_prov_cache[ "US" ][ cd_st ]
                    else:
                        cd_st_row = db.read( QUERY_SELECT_STATE_PROV,
                                             ( "US", cd_st ) )
                        if cd_st_row is not None:
                            cd_st_id = cd_st_row[0]
                            state_prov_cache[ "US" ][ cd_st ] = \
                                cd_st_id
                    if cd_st_id is not None:
                        db.write( QUERY_INSERT_CD,
                                  ( cd_st_id, cd_num,
                                    CONGRESSIONAL_SOURCE_ID,
                                    CONGRESSIONAL_SOURCE_COMMENT ) )
                        cd_id = db.get_last_id()

                # Connect the address to the district.
                if cd_id is not None:
                    cd_cache[ cd_code ] = cd_id
                    db.write( QUERY_INSERT_ADDR_CD,
                              ( addr_id, cd_id,
                                CONGRESSIONAL_SOURCE_ID,
                                CONGRESSIONAL_SOURCE_COMMENT ) )

            if cd_st != st_prov_code:
                logging.warn( "Possible state mismatch: address " +
                              "%d has state %s but CD %s-%d." %
                              ( addr_id, st_prov_code, cd_st,
                                cd_num ) )

        # Commit the changes we made for this row.
        db.finish()

        address_cache[ addr_string ] = [ addr_id, lat, lon,
                                         nation_code, cd_st, cd_num,
                                         [ org_id ] ]

        # Write out the result.
        row += [ lat, lon, cd_st, cd_num ]
        writer.writerow( row )

    logging.info( "Vendor address geocoding complete." )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
