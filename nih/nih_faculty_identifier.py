#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Identify NIH reviewers with their University of Illinois PIDMs.

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

import re

# Constants
NON_ALPHA_RE = r'[^a-zA-Z]'
non_alpha_re = re.compile( NON_ALPHA_RE ) # Yes, a global.

SPRIDEN_READ_STMT = "SELECT DISTINCT spriden_pidm " + \
    "FROM spriden_alias " + \
    "WHERE spriden_search_last_name = %s AND " + \
    "spriden_search_first_name = %s AND " + \
    "spriden_search_mi = %s;"
SPRIDEN_LOOSE_READ_STMT = "SELECT DISTINCT spriden_pidm " + \
    "FROM spriden_alias " + \
    "WHERE spriden_search_last_name = %s AND " + \
    "spriden_search_first_name = %s AND " + \
    "spriden_search_mi LIKE %s;"
SPRIDEN_NO_MID_READ_STMT = "SELECT DISTINCT spriden_pidm " + \
    "FROM spriden_alias " + \
    "WHERE spriden_search_last_name = %s AND " + \
    "spriden_search_first_name = %s;"

def searchify( name_part ):
    """
    Given a string, strip it of all non-alpha characters and upcase
    the result, to match the SPRIDEN search_*_name field usage.
    """
    if name_part is None:
        return None
    return non_alpha_re.sub( '', name_part ).upper()

def spriden_lookup( db, last, first, middle, loose_middle=False,
                    omit_middle=False ):
    """
    Get a list of candidates matching the given name search pattern.
    """
    if omit_middle or ( loose_middle and middle is None ):
        return list( db.read_many( SPRIDEN_NO_MID_READ_STMT,
                                   ( last,
                                     first ) ) )
    elif loose_middle:
        return list( db.read_many( SPRIDEN_LOOSE_READ_STMT,
                                   ( last,
                                     first,
                                     middle[0] + "%" ) ) )
    else:
        return list( db.read_many( SPRIDEN_READ_STMT,
                                   ( last,
                                     first,
                                     middle ) ) )

def main():
    """
    Look through the list of NIH reviewers and attempt to find them in
    the SPRIDEN database.  Automatically assign single matches; report
    on multiple possible candidates.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="finds NIH reviewers in SPRIDEN"
    )
    args = parser.parse_args()

    # Connect to the database.
    db_n = oria.DBConnection( offline=args.offline,
                              db_write=args.db_write, debug=args.debug )
    db_s = oria.DBConnection( offline=args.offline,
                              db_write=args.db_write, debug=args.debug )

    # We are going to iterate over the researchers, and change
    # them... probably safer to build up a list and then write it back
    # in.
    nih_to_pidm = {}

    db_n.start()
    reviewer_stmt = "SELECT id, last_name, first_name, " + \
        "middle_name FROM nih_reviewer WHERE spriden_pidm IS NULL;"
    reviewers = db_n.read_many( reviewer_stmt, () )

    for reviewer in reviewers:
        nih_id, last, first, middle = reviewer

        search_last = searchify( last )
        search_first = searchify( first )
        search_middle = searchify( middle )
        spriden_cands = spriden_lookup( db_s,
                                        search_last,
                                        search_first,
                                        search_middle )

        # If we got one match, note it and move on.
        if len( spriden_cands ) == 1:
            nih_to_pidm[ nih_id ] = spriden_cands[0][0]
            continue
        # If we got more than one, report it and move on.
        elif len( spriden_cands ) > 1:
            print "Found multiple matches for %d: " % nih_id + \
                "%s, %s %s" % ( last, first, str(middle) )
            continue

        # We got no matches.  Loosen the search.
        spriden_cands = spriden_lookup( db_s,
                                        search_last,
                                        search_first,
                                        search_middle,
                                        loose_middle=True )

        # If we got one match, note it and move on.
        if len( spriden_cands ) == 1:
            nih_to_pidm[ nih_id ] = spriden_cands[0][0]
            continue
        # If we got more than one, report it and move on.
        elif len( spriden_cands ) > 1:
            print "Found multiple matches for %d: " % nih_id + \
                "%s, %s %s" % ( last, first, str(middle) )
            continue

        # We got no matches.  Loosen the search.
        spriden_cands = spriden_lookup( db_s,
                                        search_last,
                                        search_first,
                                        search_middle,
                                        omit_middle=True )

        # If we got one match, note it and move on.
        if len( spriden_cands ) == 1:
            nih_to_pidm[ nih_id ] = spriden_cands[0][0]
            continue
        # If we got more than one, report it and move on.
        elif len( spriden_cands ) > 1:
            print "Found multiple matches for %d: " % nih_id + \
                "%s, %s %s" % ( last, first, str(middle) )
            continue

        print "No matches for %d: " % nih_id + \
            "%s, %s %s" % ( last, first, str(middle) )

    # Write out our findings.
    db_n.start()

    id_write_stmt = "UPDATE nih_reviewer SET spriden_pidm = %s " + \
        "WHERE id = %s;"
    for nih_id in nih_to_pidm:
        db_n.write( id_write_stmt, ( nih_to_pidm[ nih_id ], nih_id ) )

    db_n.finish()

    return

if __name__ == '__main__':
    main()
    exit( 0 )
