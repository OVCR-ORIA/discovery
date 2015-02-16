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

# @@@TODO: add_* functions will not re-assert expired assertions;
# there should probably be a “really do this” switch.

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"16 February 2015"
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

def add_external_org_alias( db, org_id, alias, source_id, lang='en',
                            comment=None ):
    """
    Assert an alias on an external organization.  The org and the
    source are identified by their IDs.  The alias is assumed to be
    English; a valid ISO/IANA language code should be used for any
    other language, if known.

    This will succeed silently if the alias is already asserted in
    that language, even if that alias is marked as expired.  Will
    raise MasterNonExistentEntity if the organization or source IDs
    are not valid.
    """
    update_stmt = "INSERT IGNORE INTO master_external_org_alias " + \
                  "( external_org, alias, lang, source, " + \
                  "source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s, %s );"
    _write_with_integrity( db, update_stmt,
                           ( org_id, alias, lang, source_id,
                             comment ) )

    return

def add_external_org_other_id( db, org_id, other_id, scheme_id,
                               source_id, comment=None ):
    """
    Add the given ID to the given external organization.  The org is
    specified by its integer ID, as are the scheme and source for the
    assertion.

    Will succeed but not do anything if the assertion is already
    present, even if marked as expired.  Will raise
    MasterNonExistentEntity if the organization, scheme, or source IDs
    are not valid.
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

def add_external_org_postcode( db, org_id, postcode_id, source_id,
                               comment=None ):
    """
    Associate a physical or mailing location with an organization.
    The org is specified by its integer ID, as is the source for the
    assertion.  NOTE THAT the postcode is ALSO specified by its ID!
    This arugment is NOT the postcode itself.

    Will silently succeed if the postcode is already asserted, even if
    marked as expired.  Will raise MasterNonExistentEntity if the
    organization, postcode, or source IDs are not valid.
    """
    update_stmt = "INSERT IGNORE INTO " + \
                  "master_external_org_postcode " + \
                  "( external_org, postcode, source, " + \
                  "source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s );"
    _write_with_integrity( db, update_stmt,
                           ( org_id, postcode_id, source_id,
                             comment ) )
    return

def add_external_org_relationship(): pass

def del_external_org(): pass

def del_external_org_alias( db, org_id, alias, source_id, lang='en',
                            comment=None ):
    """
    Mark the given alias as no longer valid for the external
    organization.  The org and and source are identifed by ID; the
    language is any valid ISO/IANA token (English by default).

    Will silently succeed if the alias is not asserted, or already
    marked as expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_alias " + \
                "WHERE external_org = %s AND alias = %s " + \
                "AND lang = %s AND valid_end IS NULL;"
    alias_row = db.read( find_stmt, ( org_id, alias, lang ) )
    db.finish()

    if alias_row is None:
        # We didn’t find anything to expire.
        return

    update_stmt = "UPDATE master_external_org_alias " + \
                  "SET valid_end = NOW(), source = %s"
    params = ( source_id, )

    # Update the comment if one was given, but not otherwise.
    if comment is not None:
        update_stmt += ", source_comment = %s"
        params += ( comment, )

    update_stmt += " WHERE external_org = %s AND alias = %s " + \
                   "AND lang = %s AND valid_end IS NULL;"
    params += ( org_id, alias, lang )
    _write_with_integrity( db, update_stmt, ( params ) )

    return

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

def del_external_org_postcode( db, org_id, postcode_id, source_id,
                               comment=None ):
    """

    Mark the given postcode association as no longer valid for the
    external organization.  The org and source are specified by their
    IDs, and NOTE THAT the postal code is ALSO specified by its ID!

    Will silently succeed if the postcode is not asserted, or if the
    assertion is already expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_postcode " + \
                "WHERE external_org = %s AND postcode = %s " + \
                "AND valid_end IS NULL;"
    postcode_link = db.read( find_stmt, ( org_id, postcode_id ) )
    db.finish()

    if postcode_link is None:
        # We didn’t find anything to expire.
        return

    update_stmt = "UPDATE master_external_org_postcode " + \
                  "SET valid_end = NOW(), source = %s"
    params = ( source_id, )

    # Update the comment if one was given, but not otherwise.
    if comment is not None:
        update_stmt += ", source_comment = %s"
        params += ( comment, )

    update_stmt += " WHERE external_org = %s AND postcode = %s " + \
                   "AND valid_end IS NULL;"
    params += ( org_id, postcode_id )
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_relationship(): pass
def merge_external_org(): pass
def rename_external_org(): pass
