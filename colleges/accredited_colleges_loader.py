#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Load a list of accredited colleges from the U.S. Dept. of Education
into the ORIA database.

Input data is expected to be comma-separated, as downloaded from the
DoEd.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"5 June 2014"
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import csv
import re
from urlparse import urlparse

# Regexpes for data formats encountered.
PHONE_RE = re.compile( r'^\D*1?\D*(\d{3})\D*(\d{3})\D*(\d{4}).*$' )
POST_RE = re.compile( r'^([0-9]{5})' )

def normalize_phone( phone_str ):
    """
    Normalize a phone number.  Assume NANP numbers with area code
    specified; discard extensions.  Return formatted as
    +1-nnn-nnn-nnnn.

    Does not support alphabetic phone numbers.
    """
    raw_phone = nullify_if_blank( phone_str )
    if raw_phone is None:
        return None

    phone_parts = PHONE_RE.match( raw_phone )
    if phone_parts is None:
        return None

    return "+1-%s-%s-%s" % ( phone_parts.groups() )

def normalize_web_addr( web_str ):
    """
    Normalize a Web address.  Lower-case the server; insert an HTTP
    scheme if missing; add a trailing / if there is no path given.
    """
    raw_web = nullify_if_blank( web_str )
    if raw_web is None:
        return None

    # Try parsing what we got.
    url_parts = urlparse( raw_web )

    # If there was no netloc, then we probably just had a domain name;
    # insert a scheme and try again.
    if url_parts.netloc == "":
        raw_web = "http://" + raw_web
        url_parts = urlparse( raw_web )

    if url_parts.path == "":
        raw_web = raw_web + "/"
        url_parts = urlparse( raw_web )

    return "%s://%s%s" % ( url_parts.scheme, url_parts.netloc.lower(),
                           url_parts.path )

def nullify_if_blank( data_str ):
    """
    If a string has no characters, return None, in preparation for
    inserting NULL values into the database.
    """
    data_str = data_str.strip()

    if len( data_str ) <= 0:
        return None

    return data_str

def main():
    """
    Read in a command-line specified CSV file and load it into the
    database.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads CSV DoEd data into oria DB"
    )
    parser.add_argument( "file", type=open,
                         help="CSV file to read" )
    args = parser.parse_args()

    # Caching for shared entities.
    countries = {}
    ipeds = {}
    ope = {}
    postcodes = {}

    # Connect to the database.
    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # We’re only dealing with one country right now, so...
    db.start()
    db.fetch_id( "country", "iso3166", "US", cache=countries )
    db.finish()

    # Read in the CSV file.
    csv_reader = csv.reader( args.file )
    for row in csv_reader:
        # Skip header row.
        if row[0] == 'Institution_ID':
            continue

        # Parse the row
        college_id, name, addr, city, state, postcode, phone, \
            ope_id, ipeds_id, web, __, __, __, __, __, __, __, __, \
            __, __, __, __, __, __, __ = row

        # Normalize the postcode and OPE ID, which have unfortunate
        # quotation marks.
        postcode = postcode.replace( '"', "" )
        ope_id = ope_id.replace( '"', "" )

        # Normalize keys.
        ope_id = nullify_if_blank( ope_id )
        ipeds_id = nullify_if_blank( ipeds_id )

        # Institutions without external IDs are probably not
        # interesting (e.g., medical facilities).
        if ope_id is None and ipeds_id is None:
            continue

        # Figure out the 5-digit ZIP code and find it in the database
        # if we can.
        post5 = POST_RE.match( postcode )
        if post5 is None:
            post_id = None
        else:
            db.start()
            post_id = db.fetch_id( "postcode", "postcode",
                                   post5.group(1), cache=postcodes )
            db.finish()

        # Handling OPE and IPEDS IDs is a little weird off-line, since
        # we’ll have multiple competing entries.
        if db.offline:
            if ope_id is not None and ope_id not in ope:
                ope[ ope_id ] = True
            if ipeds_id is not None and ipeds_id not in ipeds:
                ipeds[ ipeds_id ] = True

        # Normalize data values.
        addr = nullify_if_blank( addr )
        city = nullify_if_blank( city )
        state = nullify_if_blank( state )
        postcode = nullify_if_blank( postcode )
        phone = normalize_phone( phone )
        web = normalize_web_addr( web )

        # We might have either the OPE ID or the IPEDS ID or both.
        # The IPEDS ID is more common.
        if ipeds_id is None:
            db_id = db.get_or_set_id( ope,
                                      "college_university",
                                      "ope_id",
                                      { "name" : name,
                                        "ope_id" : ope_id,
                                        "ipeds_id" : ipeds_id,
                                        "address" : addr,
                                        "city" : city,
                                        "state_province" : state,
                                        "country" : countries[ "US" ],
                                        "postcode" : postcode,
                                        "postcode_ref" : post_id,
                                        "telephone" : phone,
                                        "website" : web } )
            continue

        # Write this college with the IPEDS ID as the key...
        db_id = db.get_or_set_id( ipeds,
                                  "college_university",
                                  "ipeds_id",
                                  { "name" : name,
                                    "ope_id" : ope_id,
                                    "ipeds_id" : ipeds_id,
                                    "address" : addr,
                                    "city" : city,
                                    "state_province" : state,
                                    "country" : countries[ "US" ],
                                    "postcode" : postcode,
                                    "postcode_ref" : post_id,
                                    "telephone" : phone,
                                    "website" : web } )
        # ... then set the OPE ID the same.
        ope[ ope_id ] = db_id

    return

if __name__ == '__main__':
    main()
    exit( 0 )
