#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Load information about faculty participation in NIH study sections
into the ORIA database.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"30 July 2014"
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import csv

def main():
    """
    Read in a command-line specified CSV and load it into the
    database.
    """

    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads CSV NIH data into ORIA DB"
    )
    parser.add_argument( "file", type=open,
                         help="CSV file to read" )
    args = parser.parse_args()

    # Set up some caching for shared entities.  This makes offline
    # testing much easier.  It has the possibility of a race
    # condition, but we really should be the only entity writing to
    # these tables anyway.
    depts = {}
    next_person = 1
    sections = {}

    # We won’t try to cache persons, but assume the list is sorted.
    prev_name = None

    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Read each line from the CSV file.
    csv_reader = csv.reader( args.file )
    for row in csv_reader:
        # Skip the header.
        if row[0].startswith( 'Person Name' ):
            continue

        # Normalize the row.
        for i in range(len(row)):
            if row[i] == '-':
                row[i] = None

        # Parse the row.
        last, first, middle, title, dept, __, section, role, start, \
            end, length = row

        # Find or create the study section.
        if section is None:
            section = "Unknown"

        sect_id = db.get_or_set_id( sections,
                                    "nih_study_section",
                                    "name",
                                    { "name" : section } )

        # Reuse or create the person.
        this_name = [ last, first, middle ]
        if this_name != prev_name:
            # Find or create the department.
            if dept is None or \
                    dept == 'NONE' or \
                    dept == 'MISCELLANEOUS':
                dept_id = None
            else:
                dept_id = \
                    db.get_or_set_id( depts,
                                      "nih_institution_department",
                                      "dept_name",
                                      { "dept_name" : dept } )

            db.start()

            person_stmt = "INSERT INTO nih_reviewer " + \
                "( last_name, first_name, middle_name, title, " + \
                "department ) VALUES ( %s, %s, %s, %s, %s );"
            db.write( person_stmt,
                      ( last, first, middle, title, dept_id ) )

            if args.offline or args.test:
                person_id = next_person
                next_person += 1
            else:
                person_id = db.read( "SELECT LAST_INSERT_ID();",
                                     () )[0]

            db.finish()
            prev_name = this_name

        # Create the participation.
        db.start()

        particip_stmt = "INSERT INTO " + \
            "nih_study_section_participation " + \
            "( reviewer, study_section, role, start_date, " + \
            "end_date, service_months ) " + \
            "VALUES ( %s, %s, %s, %s, %s, %s );"
        db.write( particip_stmt,
                  ( person_id, sect_id, role, start, end, length ) )

        db.finish()

    return

if __name__ == '__main__':
    main()
    exit( 0 )
