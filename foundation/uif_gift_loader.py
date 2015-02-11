#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Load data from the University of Illinois Foundation into the ORIA
database.

Input data is expected to be comma-separated, as exported from Excel
spreadsheets of UIF detail reports.

NOTE that this is NOT idempotent; since multiple discrete gifts might
look identical, we must assume that every gift is genuinely new.  If
this is run multiple times on the same input, multiple identical gifts
will be created.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"30 May 2014"
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import csv
import re

# Gift kind coding.
KIND_RE = r"^([A-Z].*) \(([0-9A-Z])\)$"

class UIFKindError( Exception ):
    """
    Raised when gift kind parsing fails.
    """
    pass

def main():
    """
    Read in a command-line specified CSV file and load it into the
    database.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads CSV UIF data into oria DB"
    )
    parser.add_argument( "file", type=open,
                         help="CSV file to read" )
    args = parser.parse_args()

    # Set up some caching for shared entities.  This makes offline
    # testing much easier.  It has the possibility of a race
    # condition, but we really should be the only entity writing to
    # these tables anyway.
    campuses = {}
    colleges = {}
    depts = {}
    efforts = {}
    ext_ents = {}
    funds = {}
    kind_cats = {}
    kinds = {}
    purposes = {}

    # Regexp for gift kind coding.
    kind_re = re.compile( KIND_RE )

    # Connect to the database.
    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Read in the CSV file.
    csv_reader = csv.reader( args.file )
    for row in csv_reader:
        # Skip header row.
        if row[0] == 'factsid':
            continue

        # Parse the row
        facts_id, donor, fy, date, gift_amt, campus_code, \
            campus_abbr, college_code, dept_code, college_banner, \
            dept_banner, campus, college, dept, fund_num, fund_name, \
            purp_code, purp_desc, kind_cat, kind_desc, effort_code, \
            effort_desc = \
            row

        if college_banner == '':
            college_banner = None
        if dept_banner == '':
            dept_banner = None

        ## Write it to the database.

        # Find or load the donor.
        donor_id = db.get_or_set_id( ext_ents,
                                     "uif_external_org",
                                     "facts_id",
                                     { "facts_id" : facts_id,
                                       "name" : donor } )

        # Find or load the internal university structures.
        campus_id = db.get_or_set_id( campuses,
                                      "uif_campus",
                                      "campus_code",
                                      { "campus_code" : campus_code,
                                        "abbreviation" : campus_abbr,
                                        "description" : campus } )
        college_id = db.get_or_set_id( colleges,
                                       "uif_college",
                                       "college_code",
                                       { "college_code" :
                                             college_code,
                                         "banner_code" :
                                             college_banner,
                                         "description" : college,
                                         "campus" : campus_id } )
        dept_id = db.get_or_set_id( depts,
                                    "uif_department",
                                    "department_code",
                                    { "department_code" : dept_code,
                                      "banner_code" : dept_banner,
                                      "description" : dept,
                                      "college" : college_id } )
        fund_id = db.get_or_set_id( funds,
                                    "uif_fund",
                                    "fund_number",
                                    { "fund_number" : fund_num,
                                      "name" : fund_name,
                                      "department" : dept_id } )

        # Find or load the gift purpose, kind, and effort.
        purpose_id = db.get_or_set_id( purposes,
                                       "uif_gift_purpose",
                                       "purpose_code",
                                       { "purpose_code" : purp_code,
                                         "description" : purp_desc } )
        kind_cat_id = db.get_or_set_id( kind_cats,
                                        "uif_gift_kind_category",
                                        "description",
                                        { "description" : kind_cat } )

        # Parse the kind description for the true code.
        kind_cand = kind_re.match( kind_desc )
        if kind_cand is None:
            raise UIFKindError, \
                "Unable to parse gift kind code %s" % ( kind_desc )
        kind_name = kind_cand.group(1)
        kind_code = kind_cand.group(2)
        kind_id = db.get_or_set_id( kinds,
                                    "uif_gift_kind",
                                    "kind_code",
                                    { "kind_code" : kind_code,
                                      "description" : kind_name,
                                      "category" : kind_cat_id } )

        effort_id = db.get_or_set_id( efforts,
                                      "uif_gift_effort",
                                      "effort_code",
                                      { "effort_code" : effort_code,
                                        "description" :
                                            effort_desc } )

        # Now load the gift itself!
        # NOTE that this is an unconditional write.  One donor might
        # make two gifts of the same size to the same fund on the same
        # date... we can’t rule it out.  SO DON’T RUN THIS TWICE!
        db.start()
        sql_stmt = "INSERT INTO uif_gift ( fiscal_year, " + \
            "gift_date, gift_amount, donor, fund, purpose, " + \
            "gift_kind, gift_effort ) VALUES ( %s, %s, %s, %s, " + \
            "%s, %s, %s, %s );"
        db.write( sql_stmt,
                  ( fy, date, gift_amt, donor_id, fund_id, purpose_id,
                    kind_id, effort_id ) )
        db.finish()

    return

if __name__ == '__main__':
    main()
    exit( 0 )
