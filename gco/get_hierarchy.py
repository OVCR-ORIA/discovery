#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Reports on the SPRIDEN entity classification hierarchy.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"28 July 2014"
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

import argparse

class HierarchyException( Exception ):
    """
    Raised if the hierarchy information is inconsistent.
    """
    pass

def flatten( root_id, childs, ents ):
    """
    Given a key into a set of tree relationships, return a list of
    paths from the given key to each leaf.
    """
    if root_id not in childs:
        return [ [ root_id ] ]

    item_lists = []
    child_ids = childs[ root_id ]
    child_ids.sort( cmp=lambda x,y: cmp( ents[x][1], ents[y][1] ) )
    for child_id in child_ids:
        sub_lists = flatten( child_id, childs, ents )
        for sub_list in sub_lists:
            sub_list.insert( 0, root_id )
            item_lists.append( sub_list )

    return item_lists

def main():
    """
    Get the hierarchy and report on it.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="reports on SPRIDEN hierarchy information"
    )
    parser.add_argument( "output", type=argparse.FileType('w'),
                         help="HTML report file" )
    args = parser.parse_args()
    outfile = args.output

    db_h = oria.DBConnection( offline=args.offline,
                              db_write=False, debug=args.debug )
    db_s = oria.DBConnection( offline=args.offline,
                              db_write=False, debug=args.debug )

    # Get the hierarchy data.
    hier_stmt = "SELECT * FROM entity_class;"
    hier_rels = db_h.read_many( hier_stmt, () )

    ent_stmt = "SELECT spriden_pidm, spriden_last_name " + \
        "FROM spriden_norm WHERE spriden_pidm = %s;"

    ents = {}
    parents = {}

    for hier_rel in hier_rels:
        ent_id, class_id = hier_rel

        if ent_id not in parents:
            parents[ ent_id ] = class_id
        elif parents[ ent_id ] != class_id:
            raise HierarchyException, \
                "Inconsistent hierarchy: " + \
                "%d has two parents: " % ( ent_id ) + \
                "%d and %d." % ( parents[ ent_id ], class_id )

        db_s.start()
        if ent_id not in ents:
            ents[ ent_id ] = db_s.read( ent_stmt, ( ent_id, ) )
        if class_id not in ents:
            ents[ class_id ] = db_s.read( ent_stmt, ( class_id, ) )
        db_s.finish()

    if args.offline:
        parents = { 510: 517,
                    511: 517,
                    513: 511,
                    516: 508,
                    517: 508 }
        ents = { 508: [ 508, "Bent" ],
                 510: [ 510, "Conlon" ],
                 511: [ 511, "Hajjar" ],
                 513: [ 513, "Desimone" ],
                 516: [ 516, "Reuter" ],
                 517: [ 517, "Brett" ] }

    # Invert the hierarchy.
    childs = {}
    for child in parents:
        parent = parents[ child ]
        if parent not in childs:
            childs[ parent ] = []
        childs[ parent ].append( child )
    # if args.offline:
    #     childs = { 508: [ 516, 517 ],
    #                511: [ 513 ],
    #                517: [ 510, 511 ]

    # Now report the hierarchy we found.
    outfile.write( """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html lang="en-US" xml:lang="en-US" dir="ltr"
  xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <title>SPRIDEN hierarchy report</title>
    <style type="text/css">
      h1     { font-size: 120%; }
      table,
      td,
      th     { border-collapse: collapse;
               border-color: black;
               border-style: solid;
               border-width: 1px; }
    </style>
  </head>
  <body>
    <h1>SPRIDEN hierarchy report</h1>
    <table>
      <tbody>
""" )

    parent_ids = childs.keys()
    parent_ids.sort( cmp=lambda x,y: cmp( ents[x][1], ents[y][1] ) )
    for parent_id in parent_ids:
        if parent_id in parents:
            continue

        item_lists = flatten( parent_id, childs, ents )

        for item_list in item_lists:
            outfile.write( "        <tr valign=\"baseline\">\n" )
            for item in item_list:
                outfile.write( "          <td>%d</td>\n" % ( item ) )
                outfile.write( "          <td>%s</td>\n" %
                               ( ents[ item ][1] ) )
            outfile.write( "        </tr>\n" )

    outfile.write( """      </tbody>
    </table>
  </body>
</html>
""" )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
