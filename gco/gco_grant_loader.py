#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Load data from the University of Illinois Office of Grants and
Contracts into the ORIA database.

Input data is expected to be comma-separated, as exported from Excel
spreadsheets of GCO detail reports.

Written for the University of Illinois.
"""

### NOTES:
# Fiscal years are given as two digits.  We treat years before 70 as
# in the 2000s, and years 70 or above as in the 1900s.

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"30 May 2014"
__version__ = 1.1

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import csv
import re

# Commercial category values
DOM_ASSN = "Corporate Associations"
DOM_COMM = "Commercial"
DOM_FOUND = "Corporate Foundations"
FORN_COMM = "Commercial (Foreign)"

# Fiscal year abbreviations
FY_RE = r"^FY([0-9][0-9])$"

class GCODateError( Exception ):
    """
    Raised when date parsing fails.
    """
    pass

def create_grant_year( db, grant_info ):
    """
    Create a grant-year combination carrying financial (year-to-date)
    information about that grant.  Because the information is YTD, we
    may want to replace an existing row instead.
    """
    # Unpack the grant info.
    grant_id, fy, budget, expense, overhead = grant_info

    # If we aren’t actually connected, our work is done.
    if db.offline:
        return

    # Start a transaction session with our DB proxy.
    db.start()

    # Look for an existing grant-year.
    sql_stmt = "SELECT * FROM gco_grant_year WHERE grant_id = %s " + \
        "AND fiscal_year = %s;"
    candidate = db.read( sql_stmt, ( grant_id, fy ), 1 )

    # Write a new year if we didn’t find it.
    if candidate is None:
        # Create a new grant-year.
        sql_stmt = "INSERT INTO gco_grant_year ( grant_id, " + \
            "fiscal_year, budget, expenditures, overhead ) " + \
            "VALUES ( %s, %s, %s, %s, %s );"
        db.write( sql_stmt,
                  ( grant_id, fy, budget, expense, overhead ) )
    else:
        # Unpack the row we found
        __, __, old_budget, old_expense, old_overhead = candidate

        # If it is at odds, replace it.
        if str(old_budget) != budget or \
                str(old_expense) != expense or \
                str(old_overhead) != overhead:
            sql_stmt = "UPDATE gco_grant_year SET budget = %s, " + \
                "expenditures = %s, overhead = %s WHERE " + \
                "grant_id = %s AND fiscal_year = %s;"
            db.write( sql_stmt,
                      ( budget, expense, overhead, grant_id,
                        fiscal_year ) )

    # Actually make the changes we wanted!
    db.finish()

    return

def main():
    """
    Read in a series of command-line specified CSV files and load them
    into the database.
    """

    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads CSV GCO data into oria DB"
    )
    parser.add_argument( "file", nargs="+", type=open,
                         help="CSV file(s) to read" )
    args = parser.parse_args()

    # Set up some caching for shared entities.  This makes offline
    # testing much easier.  It has the possibility of a race
    # condition, but we really should be the only entity writing to
    # these tables anyway.
    ext_ents = {}
    grant_categories = {}
    grant_types = {}
    grants = {}
    int_ents = {}
    investigators = {}

    # Regexp for fiscal year notation.
    fy_re = re.compile( FY_RE, flags=re.I )

    # Connect to the database.
    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Read each file.
    for csvfile in args.file:
        csv_reader = csv.reader( csvfile )
        for row in csv_reader:
            # Skip header rows.
            if row[1] == '' or row[1] == 'GRANT':
                continue

            # Parse the row
            fy, grant_num, title, start, end, type_code, type_name, \
                cat_code, cat_name, pi_code, pi_last, pi_first, \
                title_long, org_code, org_name, budget, expense, \
                overhead, ext_code, ext_name, pass_code, pass_name, \
                ext_type, ext_l1, ext_l2, ext_l3, pass_type, \
                pass_l1, pass_l2, pass_l3, us_comm, forn_comm = \
                row

            ## Write it to the database.

            # grant type
            type_id = \
                db.get_or_set_id( grant_types,
                                  "gco_grant_type",
                                  "type_code",
                                  { "type_code" : type_code,
                                    "description" : type_name } )

            # grant category
            category_id = \
                db.get_or_set_id( grant_categories,
                                  "gco_grant_category",
                                  "category_code",
                                  { "category_code" : cat_code,
                                    "description" : cat_name } )

            # internal org
            org_id = \
                db.get_or_set_id( grant_categories,
                                  "gco_internal_org",
                                  "org_code",
                                  { "org_code" : org_code,
                                    "description" : org_name } )

            # sponsor
            if us_comm == 'Y' and \
                    ( ext_l2 == DOM_COMM or
                      ext_l2 == DOM_FOUND or
                      ext_l3 == DOM_ASSN ):
                spons_us_comm = True
            else:
                spons_us_comm = False
            if forn_comm == 'Y' and ext_l2 == FORN_COMM:
                spons_forn_comm = True
            else:
                spons_forn_comm = False
            sponsor_id = \
                db.get_or_set_id( ext_ents,
                                  "gco_external_org",
                                  "banner_id",
                                  { "banner_id" : ext_code,
                                    "name" : ext_name,
                                    "category_name" : ext_type,
                                    "l1_category_name" : ext_l1,
                                    "l2_category_name" : ext_l2,
                                    "l3_category_name" : ext_l3,
                                    "us_commercial" : spons_us_comm,
                                    "foreign_commercial" :
                                        spons_forn_comm } )

            # passthrough
            passthrough_id = None
            if pass_type != '':
                if us_comm == 'Y' and \
                        ( ext_l2 == DOM_COMM or
                          ext_l2 == DOM_FOUND or
                          ext_l3 == DOM_ASSN ):
                    pthru_us_comm = True
                else:
                    pthru_us_comm = False
                if forn_comm == 'Y' and pass_l2 == FORN_COMM:
                    pthru_forn_comm = True
                else:
                    pthru_forn_comm = False
                passthrough_id = \
                    db.get_or_set_id( ext_ents,
                                      "gco_external_org",
                                      "banner_id",
                                      { "banner_id" : pass_code,
                                        "name" : pass_name,
                                        "category_name" : pass_type,
                                        "l1_category_name" : pass_l1,
                                        "l2_category_name" : pass_l2,
                                        "l3_category_name" : pass_l3,
                                        "us_commercial" :
                                            pthru_us_comm,
                                        "foreign_commercial" :
                                            pthru_forn_comm } )

            # investigator
            pi_id = \
                db.get_or_set_id( investigators,
                                  "gco_investigator",
                                  "uin",
                                  { "uin" : pi_code,
                                    "last_name" : pi_last,
                                    "first_name" : pi_first } )

            # grant
            grant_id = \
                db.get_or_set_id( grants,
                                  "gco_grant",
                                  "banner_id",
                                  { "banner_id" : grant_num,
                                    "title" : title,
                                    "start_date" : start,
                                    "end_date" : end,
                                    "grant_type" : type_id,
                                    "grant_category" : category_id,
                                    "investigator" : pi_id,
                                    "responsible_org" : org_id,
                                    "sponsor" : sponsor_id,
                                    "passthrough_sponsor" :
                                        passthrough_id,
                                    "long_title" : title_long } )

            # grant year
            year_cand = fy_re.match( fy )
            if year_cand is None:
                raise GCODateError, \
                    "Unable to parse fiscal year %s" % ( fy )
            year2 = int( year_cand.group(1) )
            if year2 < 70:
                fyear = str(year2 + 2000)
            else:
                fyear = str(year2 + 1900)
            create_grant_year( db,
                               [ grant_id, fyear, budget, expense,
                                 overhead ] )

if __name__ == '__main__':
    main()
    exit( 0 )
