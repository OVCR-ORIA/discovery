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
    args = parser.parse_args()

    # Open a connection to the ORIA database
    db = oria.DBConnection(
        offline=args.offline,
        db_write=args.db_write,
        port=3307,
        db="oria_test",
        host="127.0.0.1",
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

                # try to find master ID in cache - use "null" instead of None
                # since None can be a valid value indicating our DB doesn't
                # have that mapping
                master_id = master_id_cache.get(other_id, "null")
                if master_id == "null":
                    master_id = get_master_id_for_other_id(
                        db, other_id, scheme_id
                    )
                    master_id_cache[other_id] = master_id

                org = Organization(
                    other_id = other_id,
                    master_id = master_id,
                    name = col_map[level_prefix + "name"],
                    classification = col_map[level_prefix + "description"]
                )

                # ignore Match Gift Prog orgs (and all further children)
                if org.classification == "Match Gft Prog":
                    break

                # check whether we know about this organization (skip if not)
                # if org is Branch Office or Defunct Company, no master_id
                # needed
                if (org.master_id is None and org.classification not in \
                    ["Branch Office", "Defunct Company"]):
                    print "WARN: No record for %s id %s for %s (%s)" % \
                        (args.scheme, org.other_id, org.name, \
                        org.classification)
                    org = None
                    continue

                if None not in (prev_org, org):
                    yield (prev_org, org)

    def combine_or_merge_orgs(prev_org, org):
        """
        Combine "Branch Office" and "Defunct Company" relationships with the
        parent organization (add them as other_id and alias). If the Branch
        Office or Defunct Company were already in our oria_master as discrete
        entities, then merge them with the correct parent.
        """
        print "*** combine_or_merge:", prev_org, ",", org

        # check if we have separate (discrete) entities for prev_org and org
        if None not in (prev_org.master_id, org.master_id):
            if prev_org.master_id != org.master_id:
                # merge external orgs
                print "Merging external_orgs %s (%s) and %s (%s)" % \
                    (prev_org.name, prev_org.master_id, org.name, org.master_id)
                merge_external_org(db, prev_org.master_id, org.master_id,
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
            add_external_org_other_id(db, master_id, other_id,
                scheme_id, source_id, args.comment)
            # update the alias table with the BO or DC name
            add_external_org_alias(db, master_id, other_name, source_id,
                comment=args.comment)
            # invalidate the id cache for org
            master_id_cache.pop(other_id, None)

    def assert_subsidiary(prev_org, org):
        """
        Assert a subsidiary relationship between prev_org and org as
        'org' is a subsidiary of 'prev_org'.
        """
        print "*** subsidiary:", prev_org, ",", org

        if prev_org.master_id is not None:
            print "Add relationship: %s (%s) is subsidiary of %s (%s)" % \
                (org.name, org.master_id, prev_org.name, prev_org.master_id)
            add_external_org_relationship(db, org.master_id,
                prev_org.master_id, REL_SUBSID_ID, source_id, args.comment)
        else:
            print ("WARN: Cannot establish %s relationship to %s. " + \
                "Missing target external_org id!") % \
                (org.classification, prev_org.name)

    def assert_foundation(prev_org, org):
        """
        Assert a foundation relationship between prev_org and org as
        'org' is a foundation attached to 'prev_org'.
        """
        print "*** foundation:", prev_org, ",", org

        if prev_org.master_id is not None:
            print ("Add relationship: %s (%s) is a foundation " + \
                "attached to %s (%s)") % \
                (org.name, org.master_id, prev_org.name, prev_org.master_id)
            add_external_org_relationship(db, org.master_id,
                prev_org.master_id, REL_FOUNDATION_ID, source_id, args.comment)
        else:
            print ("WARN: Cannot establish %s relationship to %s. " + \
                "Missing target external_org id!") % \
                (org.classification, prev_org.name)

    # for all unique org pairs
    for (prev_org, org) in OrderedSet(get_org_rel_pairs()):
        # since we're processing data in order, the only reason prev_org would
        # not be found in the cache is because it's been merged with another
        # organization (at which point it was removed from the cache)
        # now retrieve the new master_id for the merged organization and use it
        if prev_org.other_id not in master_id_cache:
            master_id = get_master_id_for_other_id(
                db, prev_org.other_id, scheme_id
            )
            master_id_cache[prev_org.other_id] = master_id
            prev_org = prev_org._replace(master_id=master_id)

        if (prev_org.master_id, org.master_id) != (None, None):
            if org.classification in ["Branch Office", "Defunct Company"]:
                combine_or_merge_orgs(prev_org, org)

            elif org.classification in ["Subsidiary", "Subsidiary Co.",
                "Foreign Subsid", "U.S. Subsidiary", "Division"]:
                assert_subsidiary(prev_org, org)

            elif org.classification in ["Foundation"]:
                assert_foundation(prev_org, org)
        else:
            print ("WARN: Could not assert relationship between %s (%s) " + \
                "and %s (%s) as %s. Missing external_org IDs!") % \
                (prev_org.name, prev_org.other_id, \
                    org.name, org.other_id, org.classification)

    return

if __name__ == '__main__':
    main()
    exit(0)
