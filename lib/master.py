#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
manipulate mastered entities

Library for manipulating entities in our entity master.

All functions expect integer IDs for the entities on which they are
operating, and may do unexpected things if the ID is incorrect (but
exists).  An oria.DBConnection instance is needed for each function,
as well.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"11 February 2015"
__version__ = 1.0

from _mysql import IntegrityError

class MasterNonExistentEntity( Exception ):
    """
    Raised when attempting to operate on an entity which cannot be
    located.
    """
    pass

def _write_with_integrity( db, stmt, params ):
    """
    Generic function for writing an insert statement with foreign key
    constraints, and catching key integrity errors.
    """
    db.start()
    try:
        db.write( stmt, params )
    except IntegrityError as e:
        raise MasterNonExistentEntity( e.args )
    finally:
        db.finish()
    return

def add_external_org(): pass
def add_external_org_alias(): pass

def add_external_org_other_id( db, org_id, other_id, scheme_id,
                               source_id, comment=None ):
    """
    Add the given ID to the given external organization.  The org is
    specified by its integer ID, as are the scheme and source for the
    assertion.

    Will succeed but not do anything if the assertion is already
    present.  Will raise MasterNonExistentEntity if the organization,
    scheme, or source IDs are not valid.

    This has the interesting effect that if the other ID was
    previously asserted, then expired, this new assertion will have no
    effect.  We probably need an override switch to say yes, really
    re-assert this, even though we un-asserted it at some previous
    point.
    """
    update_stmt = "INSERT IGNORE INTO " + \
                  "master_external_org_other_id " + \
                  "( master_id, other_id, scheme, source, " + \
                  "source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s, %s );"
    _write_with_integrity( db, update_stmt,
                           ( org_id, other_id, scheme_id, source_id,
                             comment ) )

    return

def add_external_org_postcode(): pass
def add_external_org_relationship(): pass
def del_external_org(): pass
def del_external_org_alias(): pass

def del_external_org_other_id( db, org_id, other_id, scheme_id,
                               source_id, comment=None ):
    """
    Mark the given ID as no longer valid for the external
    organization.  The org is specified by its integer ID, as are the
    scheme and the source for the assertion.

    Will succeed but not do anything if the ID is not asserted, or if
    the assertion is already expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_other_id " + \
                "WHERE master_id = %s AND other_id = %s " + \
                "AND scheme = %s AND valid_end IS NULL;"
    other_link = db.read( find_stmt, ( org_id, other_id, scheme_id ) )
    db.finish()

    if other_link is None:
        # We didn’t find anything to expire.
        return

    update_stmt = "UPDATE master_external_org_other_id " + \
                  "SET valid_end = NOW(), source = %s"
    params = ( source_id, )

    # Update the comment if one was given, but not otherwise.
    if comment is not None:
        update_stmt += ", source_comment = %s"
        params += ( comment, )

    update_stmt += " WHERE master_id = %s AND other_id = %s " + \
                   "AND scheme = %s AND valid_end IS NULL;"
    params += ( org_id, other_id, scheme_id )
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_postcode(): pass
def del_external_org_relationship(): pass
def merge_external_org(): pass
def rename_external_org(): pass
