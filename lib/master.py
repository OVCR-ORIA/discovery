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

__author__ = u"Christopher R. Maden <crism@illinois.edu>, " + \
             u"Boris Capitanu <capitanu@illinois.edu>"
__date__ = u"11 March 2015"
__version__ = 1.2

from _mysql import IntegrityError

class MasterNonExistentEntity( Exception ):
    """
    Raised when attempting to operate on an entity which cannot be
    located.
    """
    pass

class SchemeNonExistent(Exception):
    """
    Raised when an unknown scheme is specified.
    """

class DataSourceNonExistent(Exception):
    """
    Raised when an unknown data source is specified.
    """

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

def add_external_org():
    """
    Unimplemented.
    """
    raise NotImplementedError

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

def add_external_org_relationship( db, org_1_id, org_2_id,
                                   rel_type_id, source_id,
                                   comment=None ):
    """
    Assert a relationship between two external organizations.  The two
    organizations, the nature of the relationship, and the source are
    all identified by their IDs.  Note that the order of the
    organizations matters!

    Will silently succeed if the relationship is already asserted,
    even if marked as expired.  Will raise MasterNonExistentEntity if
    the organizations, relationship type, or source IDs are not valid.
    """
    update_stmt = "INSERT IGNORE INTO " + \
                  "master_rel_external_external " + \
                  "( ext1, ext2, rel, source, source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s, %s );"
    _write_with_integrity( db, update_stmt,
                           ( org_1_id, org_2_id, rel_type_id,
                             source_id, comment ) )

    return

def del_external_org( db, org_id, source_id, comment=None ):
    """
    Delete (i.e., mark as no longer valid) an external organization
    and all of its properties.

    Will silently succeed if the organization is already marked as
    expired, but will raise an exception if the organization does not
    exist.
    """
    db.start()
    find_stmt = "SELECT id FROM master_external_org WHERE id = %s;"
    org_row = db.read( find_stmt, ( org_id, ) )
    db.finish()

    if org_row is None:
        # We didn’t find anything to expire.
        return

    # Delete the organization’s properties.  These will silently
    # succeed if already expired.  We run the risk of doing a little
    # extra work if the organization was already deleted, but this
    # will clean up anything inadvertently added.
    del_external_org_other_id( db, org_id, "*", None, source_id,
                               comment )
    del_external_org_postcode( db, org_id, "*", source_id, comment )
    del_external_org_relationship( db, org_id, "*", None, source_id,
                                   comment )
    del_external_org_alias( db, org_id, "*", source_id,
                            comment=comment )

    # Delete the org itself.
    update_stmt = "UPDATE master_external_org " + \
                  "SET valid_end = NOW(), source = %s"
    params = ( source_id, )

    if comment is not None:
        update_stmt += ", source_comment = %s"
        params += ( comment, )

    update_stmt += " WHERE id = %s AND valid_end IS NOT NULL;"
    params += ( org_id, )
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_alias( db, org_id, alias, source_id, lang='en',
                            comment=None ):
    """
    Mark the given alias as no longer valid for the external
    organization.  The org and and source are identifed by ID; the
    language is any valid ISO/IANA token (English by default).

    Supports a wildcard '*' on alias to expire all aliases of the
    given org_id.

    Will silently succeed if the alias is not asserted, or already
    marked as expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_alias " + \
                "WHERE external_org = %s AND valid_end IS NULL"
    params = ( org_id, )
    if alias != "*":
        find_stmt += " AND alias = %s AND lang = %s"
        params += ( alias, lang )
    find_stmt += ";"
    alias_row = db.read( find_stmt, ( params ) )
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

    update_stmt += " WHERE external_org = %s AND valid_end IS NULL"
    params += ( org_id, )
    if alias != "*":
        update_stmt += " AND alias = %s AND lang = %s"
        params += ( alias, lang )
    update_stmt += ";"
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_other_id( db, org_id, other_id, scheme_id,
                               source_id, comment=None ):
    """
    Mark the given ID as no longer valid for the external
    organization.  The org is specified by its integer ID, as are the
    scheme and the source for the assertion.

    Supports a wildcard '*' on other_id to expire all other IDs of the
    given org_id.

    Will succeed but not do anything if the ID is not asserted, or if
    the assertion is already expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_other_id " + \
                "WHERE master_id = %s AND valid_end IS NULL"
    params = ( org_id, )
    if other_id != "*":
        find_stmt += " AND other_id = %s AND scheme = %s"
        params += ( other_id, scheme_id )
    find_stmt += ";"
    other_link = db.read( find_stmt, ( params ) )
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

    update_stmt += " WHERE master_id = %s AND valid_end IS NULL"
    params += ( org_id, )
    if other_id != "*":
        update_stmt += " AND other_id = %s AND scheme = %s"
        params += ( other_id, scheme_id )
    update_stmt += ";"
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_postcode( db, org_id, postcode_id, source_id,
                               comment=None ):
    """

    Mark the given postcode association as no longer valid for the
    external organization.  The org and source are specified by their
    IDs, and NOTE THAT the postal code is ALSO specified by its ID!

    Supports a wildcard '*' on postcode_id to expire all postcodes for
    the given org_id.

    Will silently succeed if the postcode is not asserted, or if the
    assertion is already expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_external_org_postcode " + \
                "WHERE external_org = %s AND valid_end IS NULL"
    params = ( org_id, )
    if postcode_id != "*":
        find_stmt += " AND postcode = %s"
        params += ( postcode_id, )
    find_stmt += ";"
    postcode_link = db.read( find_stmt, ( params ) )
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

    update_stmt += " WHERE external_org = %s AND valid_end IS NULL"
    params += ( org_id, )
    if postcode_id != "*":
        update_stmt += " AND postcode = %s "
        params += ( postcode_id, )
    update_stmt += ";"
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def del_external_org_relationship( db, org_1_id, org_2_id,
                                   rel_type_id, source_id,
                                   comment=None ):
    """
    Mark the given inter-organizational relationship as no longer
    valid.  The orgs, relationship type, and source are all identified
    by ID.

    Supports a wildcard '*' on org_2_id, which will expire all
    relationships of which org_1_id is part, on either side!

    Will silently succeed if the relationship is unasserted, or
    already marked as expired.
    """
    db.start()
    find_stmt = "SELECT * FROM master_rel_external_external " + \
                "WHERE "
    if org_2_id == "*":
        find_stmt += "( ext1 = %s OR ext2 = %s )"
        params = ( org_1_id, org_1_id )
    else:
        find_stmt += "ext1 = %s AND ext2 = %s AND rel = %s"
        params = ( org_1_id, org_2_id, rel_type_id )
    find_stmt += " AND valid_end IS NULL;"
    org_rel = db.read( find_stmt, ( params ) )
    db.finish()

    if org_rel is None:
        # We didn’t find anything to expire.
        return

    update_stmt = "UPDATE master_rel_external_external " + \
                  "SET valid_end = NOW(), source = %s"
    params = ( source_id, )

    # Update the comment if one was given, but not otherwise.
    if comment is not None:
        update_stmt += ", source_comment = %s"
        params += ( comment, )

    update_stmt += " WHERE "
    if org_2_id == "*":
        update_stmt += "( ext1 = %s OR ext2 = %s )"
        params += ( org_1_id, org_1_id )
    else:
        update_stmt += "ext1 = %s AND ext2 = %s AND rel = %s"
        params += ( org_1_id, org_2_id, rel_type_id )
    update_stmt += " AND valid_end IS NULL;"
    _write_with_integrity( db, update_stmt, ( params ) )

    return

def get_data_source_id(db, source_name):
    """
    Get the source identifier for a given source name.

    Args:
        db: The db instance
        source_name: The source name

    Returns:
        str: The source id for the given source name

    Raises:
        DataSourceNonExistent: If an unknown source name is specified
    """
    db.start()
    find_stmt = "SELECT id FROM master_data_source " + \
                "WHERE name = %s;"
    result = db.read(find_stmt, (source_name, ))
    db.finish()

    if result is None:
        raise DataSourceNonExistent("Unknown data source: %s" % source_name)

    source_id = result[0]
    return source_id

def get_scheme_id(db, scheme_name):
    """
    Get the scheme identifier for a given scheme name.

    Args:
        db: The db instance
        scheme_name: The name of the scheme

    Returns:
        str: The scheme id for the given scheme name

    Raises:
        SchemeNonExistent: If an unknown scheme name is specified
    """
    db.start()
    find_stmt = "SELECT id FROM master_other_id_scheme " + \
                "WHERE name = %s;"
    result = db.read(find_stmt, (scheme_name, ))
    db.finish()

    if result is None:
        raise SchemeNonExistent("Unknown scheme: %s" % scheme_name)

    scheme_id = result[0]
    return scheme_id

def get_supported_data_sources(db):
    """
    Get the list of supported data sources.

    Args:
        db: The db instance

    Returns:
        list of str: The supported data sources
    """
    db.start()
    find_stmt = "SELECT name FROM master_data_source;"
    results = db.read_many(find_stmt, ())
    db.finish()

    source_names = [result[0] for result in results]
    return source_names

def get_supported_schemes(db):
    """
    Get the list of supported scheme names.

    Args:
        db: The db instance

    Returns:
        list of str: The supported scheme names
    """
    db.start()
    find_stmt = "SELECT name FROM master_other_id_scheme;"
    results = db.read_many(find_stmt, ())
    db.finish()

    scheme_names = [result[0] for result in results]
    return scheme_names

def merge_external_org():
    """
    Unimplemented.
    """
    raise NotImplementedError

def rename_external_org( db, org_id, new_name, source_id,
                         comment=None, alias_old_name=True ):
    """
    Give a new name to an external organization.  The org and the
    source are identified by their IDs.  By default, make the old
    name an English alias for the organization.

    This does not update the source or start date for the
    organization, though it will do so for the alias created, if any.

    Will silently succeed if the organization already has that name.
    Will raise MasterNonExistentEntity if the organization ID is not
    valid, or if the source ID is not valid and an alias is created.
    """
    db.start()
    find_stmt = "SELECT name FROM master_external_org " + \
                "WHERE id = %s;"
    ext_org_name = db.read( find_stmt, ( org_id, ) )
    db.finish()

    if ext_org is None:
        raise MasterNonExistentEntity(
            "There is no external organization with ID %d." % org_id
        )

    old_name = ext_org_name[0]

    update_stmt = "UPDATE master_external_org SET name = %s " + \
                  "WHERE id = %s;"
    _write_with_integrity( db, update_stmt, ( new_name, org_id ) )

    if alias_old_name:
        add_external_org_alias( db, org_id, old_name, source_id,
                                comment=comment )

    return
