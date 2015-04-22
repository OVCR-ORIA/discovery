#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Loads SPRIDEN entities into the entity master.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"22 April 2015"
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
import master
import oria

import re

BANNER_RE = r'@[0-9]{8}'
BANNER_REF_RE = r'(\s*(TERM\s*)?Use\s*)?(@[0-9]{8})'
BANNER_SCHEME = 3
PIDM_SCHEME = 1
SOURCE_COMMENT = "spriden_master.py v%0.1f" % __version__
SOURCE_ID = 1

def update_master( db, spriden_row, target_pidm=None ):
    """
    Given a row from the spriden_norm table, update the
    master_external_org and associated aliases.
    """
    pidm, banner, __, last, first, middle, __, __, __, __, __, __, \
        __ = spriden_row

    if target_pidm is None:
        target_pidm = pidm

    # See if we already have a correlation established!
    master_id = None
    pidm_corr_rows = db.read_many(
        "SELECT DISTINCT master_id " +
            "FROM master_external_org_other_id AS other, " +
            "master_other_id_scheme AS scheme " +
            "WHERE ( scheme.name = 'PIDM' " +
            "AND other.scheme = scheme.id " +
            "AND other_id IN ( %s, %s ) ) " +
            "OR ( scheme.name = 'Banner' " +
            "AND other.scheme = scheme.id AND other_id = %s )" +
            "AND other.valid_end IS NULL;",
        ( pidm, target_pidm, banner )
    )
    for pidm_corr_row in pidm_corr_rows:
        if master_id is not None:
            sys.stderr.write(
                "PIDM %d maps to " % pidm +
                "multiple existing master entities.  Skipping...\n"
            )
            return
        master_id = pidm_corr_row[0]

    # Organizations should only have “last names.”
    if ( first is not None and first.strip() ) or \
       ( middle is not None and middle.strip() ):
        sys.stderr.write(
            "PIDM %d is non-person, but " % pidm +
            "has first or middle name:\n    " +
            "%s, %s, %s\n" % ( last, first, middle )
        )

    # Get all the known names for this entity.
    names = [ last ]
    alias_rows = db.read_many(
        "SELECT spriden_last_name FROM spriden_alias " +
            "WHERE spriden_pidm = %s;",
        ( pidm, )
    )
    for alias_row in alias_rows:
        names.append( alias_row[0] )

    # Look for this entity by its name(s) in the existing external
    # organizations.  Report ambiguity.
    if master_id is None:
        for name in names:
            org_rows = db.read_many(
                "SELECT DISTINCT org.id " +
                    "FROM master_external_org AS org, " +
                    "master_external_org_alias AS alias " +
                    "WHERE alias.external_org = org.id " +
                    "AND ( name = %s OR alias = %s )" +
                    "AND org.valid_end IS NULL " +
                    "AND alias.valid_end IS NULL;",
                ( name, name )
            )
            for org_row in org_rows:
                if master_id is not None:
                    sys.stderr.write(
                        "PIDM %d has ambiguous " % pidm +
                        "master name matches.  Skipping...\n"
                    )
                    return
                master_id = org_row[0]

    # If we still haven’t found a match, make a new entity.
    if master_id is None:
        # Note that we pop the primary name out of the list at this
        # point, so it won’t get added as an alias.
        master_id = master.add_external_org( db, names.pop(0),
                                             SOURCE_ID,
                                             SOURCE_COMMENT )

    # Update aliases.  (For lack of better information, assume all
    # names are in English.)
    for name in names:
        master.add_external_org_alias( db, master_id, name, SOURCE_ID,
                                       comment=SOURCE_COMMENT )

    # Note the correlation between the PIDM, the Banner ID, and the
    # master entity.
    master.add_external_org_other_id( db, master_id, pidm,
                                      PIDM_SCHEME, SOURCE_ID,
                                      SOURCE_COMMENT )
    master.add_external_org_other_id( db, master_id, banner,
                                      BANNER_SCHEME, SOURCE_ID,
                                      SOURCE_COMMENT )

    return

def main():
    """
    Connect to the database and correlate entities.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description=
            "correlates SPRIDEN data with the ORIA entity master"
    )
    args = parser.parse_args()

    # These names imply “read” and “write,” but we really use db_w for
    # other, smaller reads as well, when db_r has an active
    # long-running cursor.  Let’s say the w stands for “working.”
    if args.db is None:
        args.db = oria.DB_BASE_TEST
    db_r = oria.DBConnection( db=args.db, offline=args.offline,
                              db_write=args.db_write, debug=args.debug )
    db_w = oria.DBConnection( db=args.db, offline=args.offline,
                              db_write=args.db_write, debug=args.debug )

    # Regexp for Banner IDs.
    banner_re = re.compile( BANNER_RE )
    banner_ref_re = re.compile( BANNER_REF_RE )

    spriden_ctr = 0
    deferred_pidms = []

    # Get every non-person from the spriden_norm table.
    spriden_rows = db_r.read_many(
        "SELECT norm.* FROM spriden_norm AS norm, " +
            "spriden_raw AS raw WHERE raw.spriden_entity_ind = 'C' " +
            "AND norm.spriden_pidm = raw.spriden_pidm",
        ()
    )

    for spriden_row in spriden_rows:
        # If the last name contains a Banner ID, defer it.
        if banner_re.search( spriden_row[3] ):
            deferred_pidms.append( spriden_row[0] )
            continue

        update_master( db_w, spriden_row )

        spriden_ctr += 1
        if args.test and spriden_ctr >= 100:
            break

        if spriden_ctr % 10000 == 0:
            print( spriden_ctr )

    print( spriden_ctr )
    spriden_ctr = 0

    # Now handle the items that had cross-references.
    for pidm in deferred_pidms:
        db_r.start()
        spriden_row = db_r.read(
            "SELECT * FROM spriden_norm WHERE spriden_pidm = %s;",
            ( pidm, )
        )

        last = spriden_row[3]

        # Look for Banner cross-references in the “last name.”
        ref_matches = banner_ref_re.search( last )
        if ref_matches is None:
            sys.stderr.write(
                "Unable to handle Banner cross-reference from " +
                "PIDM %d.\n" % spriden_row[0]
            )
            db_r.finish()
            continue
        target_banner = ref_matches.group(3)

        # Find the PIDM by Banner ID.
        target_row = db_r.read(
            "SELECT spriden_pidm FROM spriden_norm " +
                "WHERE banner_id = %s;",
            ( target_banner, )
        )
        if target_row is None:
            sys.stderr.write(
                "Unable to handle Banner ID %s " % target_banner +
                "(cross-reference from " +
                "PIDM %d).\n" % spriden_row[0]
            )
            db_r.finish()
            continue
        target_pidm = target_row[0]
        db_r.finish()

        # Adjust the row to strip the cross-reference out of the “last
        # name.”
        spriden_list = list(spriden_row)
        spriden_list[3] = banner_ref_re.sub( '', last )
        spriden_row = tuple(spriden_list)

        # Now update the master with this cross-reference row.
        update_master( db_w, spriden_row, target_pidm )

        spriden_ctr += 1
        if args.test and spriden_ctr >= 100:
            break

        if spriden_ctr % 10000 == 0:
            print( spriden_ctr )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
