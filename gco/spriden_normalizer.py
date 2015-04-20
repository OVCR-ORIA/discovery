#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Iterates over the raw SPRIDEN data, creating normalized structures.

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

import re

UIN_RE = r'^[0-9]{9}$'

class SpridenUINError( Exception ):
    """
    Raised when a conflicting UIN is found in the system.
    """
    pass

def main():
    """
    Connect to the database and do lots of stuff.
    """

    # Parse user options.
    parser = oria.ArgumentParser(
        description="massages SPRIDEN data in the ORIA DB"
    )
    args = parser.parse_args()

    if args.db is None:
        args.db = oria.DB_BASE_TEST
    db_r = oria.DBConnection( db=args.db, offline=args.offline,
                              db_write=args.db_write, debug=args.debug )
    db_w = oria.DBConnection( db=args.db, offline=args.offline,
                              db_write=args.db_write, debug=args.debug )

    # Regexp for UINs.
    uin_re = re.compile( UIN_RE )

    # Read every row in the spriden_raw table.
    spriden_rows = db_r.read_many( "SELECT * FROM spriden_raw;",
                                   () )

    spriden_ctr = 0
    for spriden_row in spriden_rows:
        # Parse the result.
        pidm, id, last, first, mi, __, __, activity, __, __, \
            search_last, search_first, search_mi, soundex_last, \
            soundex_first, ntyp, __, create_date, __, __, __, __, \
            __, __, __ = spriden_row

        db_w.start()

        # Find the corresponding row in spriden_norm.
        spr_norm = db_w.read( "SELECT * FROM spriden_norm " +
                                  "WHERE spriden_pidm = %s;",
                              ( pidm, ) )

        # Maybe we didn’t find a normalized record!
        if spr_norm is None:
            db_w.write( "INSERT INTO spriden_norm " +
                            "( spriden_pidm, spriden_last_name, " +
                            "spriden_first_name, spriden_mi, " +
                            "spriden_activity_date, " +
                            "spriden_search_last_name, " +
                            "spriden_search_first_name, " +
                            "spriden_search_mi, " +
                            "spriden_soundex_last_name, " +
                            "spriden_soundex_first_name, " +
                            "spriden_create_date ) " +
                            "VALUES ( %s, %s, %s, %s, %s, %s, %s, " +
                            "%s, %s, %s, %s );",
                        ( pidm, last, first, mi, activity,
                          search_last, search_first, search_mi,
                          soundex_last, soundex_first, create_date ) )
            banner = None
            uin = None
            norm_activity = activity
            norm_create = create_date
        else:
            # Parse the normalized result.
            __, banner, uin, __, __, __, norm_activity, __, __, __, \
                __, __, norm_create = spr_norm

        update_clauses = []
        update_params = []

        # Set the UIN if appropriate.
        uin_cand = uin_re.match( id )
        if uin_cand is not None:
            if uin is not None and id != uin:
                # raise SpridenUINError, \
                #     "At PIDM %s, " % ( pidm ) + \
                #     "new UIN %s " % ( id ) + \
                #     "does not match existing %s." % ( uin )
                sys.stderr.write( "At PIDM %s, " % ( pidm ) +
                                  "new UIN %s " % ( id ) +
                                  "does not match existing " +
                                  "%s.\n" % ( uin ) )
            update_clauses.append( "uin = %s" )
            update_params.append( id )

        # If the activity date is newer, update the main record with
        # that.
        if args.debug:
            print "*** Comparing activity dates:"
            print "    %s (%s)" % (norm_activity, type(norm_activity))
            print "    %s (%s)" % (activity, type(activity))
        if norm_activity > activity:
            update_clauses.append( "spriden_activity_date = %s" )
            update_params.append( norm_activity )

        # If the create date is older, update the main record with
        # that.
        if norm_create is not None and \
                ( create_date is None or norm_create < create_date ):
            update_clauses.append( "spriden_create_date = %s" )
            update_params.append( norm_create )

        if update_clauses:
            update_stmt = "UPDATE spriden_norm SET " + \
                ", ".join( update_clauses ) + \
                " WHERE spriden_pidm = %s;"
            update_params.append( pidm )
            db_w.write( update_stmt, update_params )

        # Create a spriden_alias row.
        db_w.write( "INSERT IGNORE INTO spriden_alias " +
                        "( spriden_pidm, spriden_last_name, " +
                        "spriden_first_name, spriden_mi, " +
                        "spriden_search_last_name, " +
                        "spriden_search_first_name, " +
                        "spriden_search_mi, " +
                        "spriden_soundex_last_name, " +
                        "spriden_soundex_first_name, " +
                        "spriden_ntyp_code ) " +
                        "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, " +
                        "%s, %s );",
                    ( pidm, last, first, mi, search_last,
                      search_first, search_mi, soundex_last,
                      soundex_first, ntyp ) )

        db_w.finish()

        spriden_ctr += 1
        if args.test and spriden_ctr >= 100:
            break

    return

if __name__ == '__main__':
    main()
    exit( 0 )
