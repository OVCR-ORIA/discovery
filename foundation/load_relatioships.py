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
from orderedset import OrderedSet

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

    # Parse arguments and figure out which DB to use
    args = parser.parse_args()

    if args.db is None:
        args.db = oria.DB_BASE_TEST

    if args.debug:
        print "*** Using database: %s (port %s)" % (args.db, args.port)

    # Open a connection to the ORIA database
    db = oria.DBConnection(
        db = args.db,
        port = args.port,
        offline=args.offline,
        db_write=args.db_write,
        debug=args.debug
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

    # Lookup table for other_id -> master_id mapping (improves performance)
    master_id_cache = {}

    # Records the total number of rows that were changed as a result of running
    # this script
    total_affected_rows = 0

    Organization = namedtuple(
        "Organization",
        "other_id master_id name classification"
    )

    def get_org_rel_pairs():
        """
        Parses the CSV and generates pairs of organizations based on the
        hierarchy specified in the CSV, observing the rules established for
        asserting organizational relationships.
        """
        # Read each row.
        for row in csv_reader:
            # Extract a mapping from column names to row values
            col_map = {c.strip(): v.strip() for c,v in zip(csv_col_names, row)}

            org = None
            for i in range(len(col_map)): # can have len(col_map) max levels
                prev_org = org

                level_prefix = "level{}_".format(i)
                other_id = col_map.get(level_prefix + "id")

                # break when exhausted all levels for this row
                if other_id is None or len(other_id) == 0:
                    break

                org = Organization(
                    other_id = other_id,
                    master_id = None,
                    name = col_map[level_prefix + "name"],
                    classification = col_map[level_prefix + "description"]
                )

                # ignore Match Gift Prog orgs (and all further children)
                if org.classification == "Match Gft Prog":
                    break

                if None not in (prev_org, org):
                    yield (prev_org, org)

    def combine_or_merge_orgs(prev_org, org):
        """
        Combine "Branch Office" and "Defunct Company" relationships with the
        parent organization (add them as other_id and alias). If the Branch
        Office or Defunct Company were already in our oria_master as discrete
        entities, then merge them with the correct parent.
        """
        n = 0  # number of rows affected

        # check to ensure we have records for at least one organizationsin our DB
        if (prev_org.master_id, org.master_id) == (None, None):
            return 0

        if args.debug: print "*** combine_or_merge:", prev_org, ",", org

        # check if we have separate (discrete) entities for prev_org and org
        if None not in (prev_org.master_id, org.master_id):
            if prev_org.master_id != org.master_id:
                # merge external orgs
                print "Merging external_orgs %s (%s) and %s (%s)" % \
                    (prev_org.name, prev_org.master_id, org.name, org.master_id)
                n += merge_external_org(db, prev_org.master_id, org.master_id,
                    source_id, args.comment)
                # invalidate cache since org was merged into prev_org
                del master_id_cache[org.other_id]
            else:
                # nothing to do; prev_org and org are already combined
                pass
        else:
            # otherwise, find the correct direction for the relationship
            if prev_org.master_id is not None:
                master_id = prev_org.master_id
                master_name = prev_org.name
                other_id = org.other_id
                other_name = org.name
            else:
                master_id = org.master_id
                master_name = org.name
                other_id = prev_org.other_id
                other_name = prev_org.name

            print "Add other_id mapping and alias for %s (%s) as %s (%s)" % \
                (master_name, master_id, other_name, other_id)
            # assert the BO or DC id as "other_id" for the external_org
            n += add_external_org_other_id(db, master_id, other_id,
                    scheme_id, source_id, args.comment)
            # update the alias table with the BO or DC name
            n += add_external_org_alias(db, master_id, other_name, source_id,
                    comment=args.comment)
            # invalidate the id cache for org
            master_id_cache.pop(other_id, None)

        return n

    def assert_subsidiary(prev_org, org):
        """
        Assert a subsidiary relationship between prev_org and org as
        'org' is a subsidiary of 'prev_org'.
        """
        n = 0  # number of rows affected

        if args.debug: print "*** subsidiary:", prev_org, ",", org

        # check to ensure we have records for both organizations in our DB
        if None in (prev_org.master_id, org.master_id):
            for o in (prev_org, org):
                # if not, report them
                if o.master_id is None:
                    print "WARN: No record for %s id %s for %s (%s)" % \
                        (args.scheme, o.other_id, o.name, \
                        o.classification)
            return 0

        print "Add relationship: %s (%s) is subsidiary of %s (%s)" % \
            (org.name, org.master_id, prev_org.name, prev_org.master_id)
        n += add_external_org_relationship(db, org.master_id,
                prev_org.master_id, REL_SUBSID_ID, source_id, args.comment)

        return n

    def assert_foundation(prev_org, org):
        """
        Assert a foundation relationship between prev_org and org as
        'org' is a foundation attached to 'prev_org'.
        """
        n = 0  # number of rows affected

        if args.debug: print "*** foundation:", prev_org, ",", org

        # check to ensure we have records for both organizations in our DB
        if None in (prev_org.master_id, org.master_id):
            for o in (prev_org, org):
                # if not, report them
                if o.master_id is None:
                    print "WARN: No record for %s id %s for %s (%s)" % \
                        (args.scheme, o.other_id, o.name, \
                        o.classification)
            return 0

        print ("Add relationship: %s (%s) is a foundation " + \
            "attached to %s (%s)") % \
            (org.name, org.master_id, prev_org.name, prev_org.master_id)
        n += add_external_org_relationship(db, org.master_id,
                prev_org.master_id, REL_FOUNDATION_ID, source_id, args.comment)

        return n

    def query_id_cache(other_id):
        """
        Query the cache to retrieve the master_id associated with other_id.
        In case of a cache miss, retrieve the ID from the DB and update cache.
        """
        # try to find master ID in cache - use "null" instead of None
        # since None can be a valid value indicating our DB doesn't
        # have that mapping
        master_id = master_id_cache.get(other_id, "null")
        if master_id == "null":
            master_id = get_master_id_for_other_id(db, other_id, scheme_id)
            master_id_cache[other_id] = master_id

        return master_id


    # for all unique org pairs
    for (prev_org, org) in OrderedSet(get_org_rel_pairs()):
        # retrieve the master_id for each organization (if exists)
        prev_org = prev_org._replace(master_id=query_id_cache(prev_org.other_id))
        org = org._replace(master_id=query_id_cache(org.other_id))

        if org.classification in ["Branch Office", "Defunct Company"]:
            total_affected_rows += combine_or_merge_orgs(prev_org, org)

        elif org.classification in ["Subsidiary", "Subsidiary Co.",
            "Foreign Subsid", "U.S. Subsidiary", "Division"]:
            total_affected_rows += assert_subsidiary(prev_org, org)

        elif org.classification in ["Foundation"]:
            total_affected_rows += assert_foundation(prev_org, org)

    print "Total affected rows: {:,}".format(total_affected_rows)

    return

if __name__ == '__main__':
    main()
    exit(0)
