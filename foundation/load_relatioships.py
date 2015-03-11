#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Loads a set of entity matches into the oria master DB.

Written for the University of Illinois
"""

__author__ = u"Boris Capitanu <capitanu@illinois.edu>"
__date__ = u"9 March 2015"
__version__ = 1.0

# Adjust the load path for common data loading operations
import sys
sys.path.append('../lib')

import argparse
import csv
import oria
from master import *
from collections import namedtuple

def main():
    """
    Read in a command-line specified CSV file and load the set of
    corporate relationships into the oria master DB
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="load corporate relationships into oria master DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--scheme",
        metavar="NAME",
        required=True,
        help="Specifies the scheme name for the matched entities."
    )
    parser.add_argument(
        "--source",
        metavar="NAME",
        required=True,
        help="Specifies the data source name for the corporate relationships."
    )
    parser.add_argument(
        "-c", "--comment",
        help="Specifies a comment to be associated with the relationships."
    )
    parser.add_argument(
        "csvfile",
        type=open,
        help="CSV file to read"
    )
    args = parser.parse_args()

    # Open a connection to the ORIA database
    db = oria.DBConnection(
        offline=args.offline,
        db_write=args.db_write,
        debug=args.debug,
        port=3307,
        host="127.0.0.1"
    )

    # Get the scheme_id for the given scheme name
    try:
        scheme_id = get_scheme_id(db, args.scheme)
    except SchemeNonExistent:
        print "ERR: Unknown scheme: %s" % args.scheme
        # Display the list of supported schemes
        supported_schemes = get_supported_schemes(db)
        print "Supported schemes: %s" % supported_schemes
        exit(1)

    if args.debug:
        print "*** Using scheme: %s (id: %s)" % (args.scheme, scheme_id)

    # Get the source_id for the given data source name
    try:
        source_id = get_data_source_id(db, args.source)
    except DataSourceNonExistent:
        print "ERR: Unknown data source: %s" % args.source
        # Display the list of supported data sources
        supported_data_sources = get_supported_data_sources(db)
        print "Supported data sources: %s" % supported_data_sources
        exit(2)

    if args.debug:
        print "*** Using data source: %s (id: %s)" % (args.source, source_id)

    # Open input CSV
    csv_reader = csv.reader(args.csvfile)

    # Read the header (assume header exists)
    csv_col_names = csv_reader.next()

    # Get the IDs for the different relationships we need
    REL_SUBSID_ID = get_relationship_type_id(db, "subsidiary")
    REL_FOUNDATION_ID = get_relationship_type_id(db, "foundation")

    total = 0
    failed = 0

    master_id_cache = {}

    Organization = namedtuple(
        "Organization",
        "other_id master_id name classification"
    )

    # Read each row.
    for row in csv_reader:
        # Extract a mapping from column names to row values (and strip whitespaces)
        col_map = { c.strip(): v.strip() for c,v in zip(csv_col_names, row) }

        prev_org = None
        for i in range(len(col_map)):  # can only have len(col_map) max levels
            level_prefix = "level{}_".format(i)
            other_id = col_map.get(level_prefix + "id")

            # break when exhausted all levels for this row
            if other_id is None or len(other_id) == 0:
                break

            # try to find master ID in cache - use "null" instead of None since
            # None can be a valid value indicating our DB doesn't have that mapping
            master_id = master_id_cache.get(other_id, "null")
            if master_id == "null":
                master_id = get_master_id_for_other_id(db, other_id, scheme_id)
                master_id_cache[other_id] = master_id

            org = Organization(
                other_id = other_id,
                master_id = master_id,
                name = col_map[level_prefix + "name"],
                classification = col_map[level_prefix + "description"]
            )

            # ignore Match Gift Prog organizations (and all further children)
            if org.classification == "Match Gft Prog":
                break

            # check whether we know about this organization (skip if we don't)
            # if org is Branch Office or Defunct Company, no master_id required
            if (org.master_id is None and
                org.classification not in ["Branch Office", "Defunct Company"]):
                print "WARN: No record for %s id %s for %s (%s)" % \
                    (args.scheme, org.other_id, org.name, org.classification)
                prev_org = None
                continue

            if (prev_org is not None and
                (prev_org.master_id, org.master_id) != (None, None)):
                # print prev_org, " <-> ", org

                if org.classification in ["Branch Office", "Defunct Company"]:
                    # check if we have separate (discrete) entities for prev_org and org
                    if (None not in (prev_org.master_id, org.master_id) and
                        prev_org.master_id != org.master_id):
                        # merge external orgs
                        print "*** need to merge external_orgs %s and %s" % (prev_org.master_id, org.master_id)
                        # todo: invalidate the cache for the IDs
                    else:
                        if prev_org.master_id is not None:
                            master_id = prev_org.master_id
                            other_id = org.other_id
                            name = org.name
                        else:
                            master_id = org.master_id
                            other_id = prev_org.other_id
                            name = prev_org.name

                        print "Add other_id mapping: %s -> %s" % (other_id, master_id)
                        # assert the BO or DC id as "other_id" for the external_org
                        add_external_org_other_id(db, master_id, other_id,
                            scheme_id, source_id, args.comment)
                        # update the alias table with the BO or DC name
                        add_external_org_alias(db, master_id, name, source_id,
                            comment=args.comment)

                    # set the master_id for this BO or DC
                    if org.master_id is None:
                        org = org._replace(master_id=prev_org.master_id)

                elif org.classification in ["Subsidiary", "Subsidiary Co.",
                    "Foreign Subsid", "U.S. Subsidiary", "Division"]:
                    if prev_org.master_id is not None:
                        print "Add relationship: %s (%s) is subsidiary of %s (%s)" % (org.name, org.master_id, prev_org.name, prev_org.master_id)
                        add_external_org_relationship(db, org.master_id,
                            prev_org.master_id, REL_SUBSID_ID, source_id, args.comment)

                elif org.classification in ["Foundation"]:
                    if prev_org.master_id is not None:
                        print "Add relationship: %s (%s) is a foundation attached to %s (%s)" % (org.name, org.master_id, prev_org.name, prev_org.master_id)
                        add_external_org_relationship(db, org.master_id,
                            prev_org.master_id, REL_FOUNDATION_ID, source_id, args.comment)

            prev_org = org

        try:
            pass
        except Exception as e:
            print ("ERR: Could not assert match: other(%s) -> org(%s)!\n" + \
                  "Reason: %s") % (other_id, meo_id, e)
            failed += 1
        finally:
            total += 1

    print "All done. [total: {:0,d}, success: {:1,d}, failed: {:2,d}]".format(
        total, total-failed, failed
    )
    return

if __name__ == '__main__':
    main()
    exit(0)
