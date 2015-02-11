#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Attempts to identify colleges and universities (especially
minority-serving institutions) in the SPRIDEN data.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"16 July 2014"
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import re

IGNORES = [ "College",
            "Institute",
            "of",
            "The",
            "the",
            "University" ]
INSUFFICIENT = [ "American",
                 "Big",
                 "California",
                 "City",
                 "De",
                 "Del",
                 "East",
                 "Eastern",
                 "El",
                 "Fort",
                 "Inter",
                 "La",
                 "Lone",
                 "Long",
                 "Los",
                 "Mt",
                 "New",
                 "North",
                 "Northern",
                 "Point",
                 "Saint",
                 "San",
                 "Santa",
                 "South",
                 "Southern",
                 "St.",
                 "West" ]
INIT_RE = r'^[A-Z]\.?$'
NONALPHA_RE = r'^\W+$'
COLLEGE_PIDM = 316486 # Yes, hardcoding a database value...

init_re = re.compile( INIT_RE )         # Yes, they’re globals...
nonalpha_re = re.compile( NONALPHA_RE )

def find_root( school_name ):
    """
    Attempt to find the distinctive heart of a school’s name — usually
    the first word, but not always.  Returns a SQL match expression.
    """
    words = school_name.split()

    match_expr = ""
    while words:
        candidate = words.pop(0)

        if candidate in IGNORES or nonalpha_re.match( candidate ):
            if not match_expr.endswith('%'):
                if match_expr:
                    match_expr += ' '
                match_expr += '%'
            continue

        if match_expr and not match_expr.endswith( ' ' ):
            match_expr += ' '
        match_expr += candidate

        if candidate in INSUFFICIENT or init_re.match( candidate ):
            continue

        break

    if not match_expr.endswith('%'):
        match_expr += " %"

    return match_expr

def main():
    """
    Do the heavy lifting: identify colleges of interest, find the root
    names, then attempt to find their SPRIDEN name(s).
    """

    # Parse user options.
    parser = oria.ArgumentParser(
        description="identifies colleges in SRPIDEN data in the ORIA DB"
    )
    parser.add_argument( "-f", "--filter", nargs="+", type=str,
                         help="name(s) of column(s) to require TRUE" )
    args = parser.parse_args()

    db_coll = oria.DBConnection( offline=args.offline,
                                 db_write=args.db_write,
                                 debug=args.debug )
    db_cand = oria.DBConnection( offline=args.offline,
                                 db_write=args.db_write,
                                 debug=args.debug )

    # Get all the schools of interest from the list of colleges.
    coll_stmt = "SELECT name, master_id FROM college_university"
    if args.filter:
        coll_stmt += " WHERE " + \
            " AND ".join([ filter + " IS TRUE"
                           for filter in args.filter ])
    coll_stmt += ";"

    colleges = db_coll.read_many( coll_stmt, () )

    print "\t".join( [ "college ID", "college name", "search string",
                       "candidate PIDM", "candidate name" ] )

    for college in colleges:
        name, id = college

        # Get the root of the name for searching.
        name_root = find_root( name )

        # Get all the candidate PIDMs and names that are universities.
        cand_stmt = "SELECT DISTINCT a.spriden_pidm, " + \
            "a.spriden_last_name " + \
            "FROM spriden_alias AS a, entity_class AS ec, " + \
            "spriden_norm AS s " + \
            "WHERE a.spriden_pidm = ec.entity_pidm " + \
            "AND ec.class_pidm = s.spriden_pidm " + \
            "AND a.spriden_last_name LIKE %s " + \
            "AND s.spriden_pidm = %s " + \
            "ORDER BY a.spriden_pidm;"
        candidates = db_cand.read_many( cand_stmt,
                                        ( name_root, COLLEGE_PIDM ) )

        # Report the results.
        cand_ctr = 0
        for candidate in candidates:
            cand_id, cand_name = candidate
            print "\t".join( [ str(id),
                               name,
                               name_root,
                               str(cand_id),
                               cand_name ] )
            cand_ctr += 1
        if cand_ctr == 0:
            print "\t".join( [ str(id), name, name_root, "", "" ] )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
