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

__author__ = u"""Christopher R. Maden <crism@illinois.edu>
Boris Capitanu <capitanu@illinois.edu>"""
__date__ = u"17 March 2015" # Éirinn go Brách!
__version__ = 1.2

from _mysql import IntegrityError

class DataSourceNonExistent(Exception):
    """
    Raised when an unknown data source is specified.
    """
    pass

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
    pass

class RelationshipNonExistent(Exception):
    """
    Raised when an unknown relationship is specified.
    """

class RelationshipNonExistent(Exception):
    """
    Raised when an unknown relationship is specified.
    """

def _write_with_integrity( db, stmt, params ):
    """
    Generic function for writing an insert statement with foreign key
    constraints, and catching key integrity errors.

    Args:
        db: an oria.DBConnection instance
        stmt: a SQL write statement with placeholders (%s)
        params: a tuple for substitution in the statement

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if the SQL statement fails due to
            reference integrity constraints
    """
    db.start()
    try:
        db.write( stmt, params )
    except IntegrityError as e:
        raise MasterNonExistentEntity( e.args )
    finally:
        db.finish()
    return

def add_external_org( db, name, source_id, comment=None, edu=False,
                      biz=False, org=False, gov=False ):
    """
    Add an external organization with the given name and attributes.

    Args:
        db: an oria.DBConnection instance
        name: string naming the organization
        source_id: the ID of the source for this organization’s
            information
        comment: a descriptive comment about the source of this
            organization’s information
        edu: True if the organization is educational
        biz: True if the organization is a business
        org: True if the organization is non-profit
        gov: True if the organization is a government body

    Returns:
        long: the ID of the newly-created organization
    """
    update_stmt = "INSERT INTO master_external_org " + \
                  "( name, educational, business, nonprofit, " + \
                  "government, source, source_comment ) " + \
                  "VALUES ( %s, %s, %s, %s, %s, %s, %s );"
    params = ( name, edu, biz, org, gov, source_id, comment )

    _write_with_integrity( db, update_stmt, params )

    db.start()
    org_id = db.read( "SELECT LAST_INSERT_ID()", () )[0]
    db.finish()

    return org_id

def add_external_org_alias( db, org_id, alias, source_id, lang="en",
                            comment=None ):
    """
    Assert an alias on an external organization.

    This will succeed silently if the alias is already asserted in
    that language, even if that alias is marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        alias: string giving the desired alias
        source_id: the ID of the source for this alias
        lang: a valid ISO/IANA language code for the alias (English by
            default)
        comment: a descriptive comment about the source of this alias

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id or source_id is not valid
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
    Add the given ID to the given external organization.

    Will succeed but not do anything if the assertion is already
    present, even if marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        other_id: string giving the ID in some other scheme
        scheme_id: the ID of the other scheme
        source_id: the ID of the source for this information
        comment: a descriptive comment about the source of this
            information

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id, scheme_id, or source_id is
            not valid
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

    Will silently succeed if the postcode is already asserted, even if
    marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        postcode_id: the ID of the postcode (NOT the postcode itself!)
        source_id: the ID of the source for this postcode
        comment: a descriptive comment about the source of this
            postcode

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id, postcode_id, or source_id
            is not valid
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
    Assert a relationship between two external organizations.  Note
    that the order of the organizations matters!

    Will silently succeed if the relationship is already asserted,
    even if marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_1_id: the ID of the first target organization
        org_2_id: the ID of the second target organization
        rel_type_id: the ID of the nature of the relationship
        source_id: the ID of the source for this relationship
        comment: a descriptive comment about the source of this
            relationship information

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_1_id, org_2_id, rel_type_id,
            or source_id is not valid
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
    expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        source_id: the ID of the source for the non-existence of the
            organization
        comment: a descriptive comment about the source of this
            deletion

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id or source_id is not valid
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

def del_external_org_alias( db, org_id, alias, source_id, lang="en",
                            comment=None ):
    """
    Mark the given alias as no longer valid for the external
    organization.

    Will silently succeed if the alias is not asserted, or already
    marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        alias: the alias to remove (in conjunction with lang), or '*'
            to remove all aliases from the organization
        source_id: the ID of the source for the removal
        lang: the language of the alias to remove (ideally, a valid
            ISO/IANA language code; ignored if alias is '*')
        comment: a descriptive comment about the source of this
            removal

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id or source_id is not valid
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
    organization.

    Will succeed but not do anything if the ID is not asserted, or if
    the assertion is already expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        other_id: the string giving the ID to remove (in conjunction
            with scheme_id), or '*' to remove all other IDs
        scheme_id: the ID of the scheme in which to remove other_id
            (ignored if other_id is '*')
        source_id: the ID of the source of the removal assertion
        comment: a descriptive comment about the source of the removal

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id, scheme_id, or source_id is
            not valid
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
    external organization.

    Will silently succeed if the postcode is not asserted, or if the
    assertion is already expired.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        postcode_id: the ID of the postcode to remove (NOT the
            postcode itself!)
        source_id: the ID of the source for the removal information
        comment: a descriptive comment about the source of the removal

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id, postcode_id, or source_id
            is not valid
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
    valid.

    Will silently succeed if the relationship is unasserted, or
    already marked as expired.

    Args:
        db: an oria.DBConnection instance
        org_1_id: the ID of the first target organization (or the only
            one if org_2_id is '*')
        org_2_id: the ID of the second target organization, or '*' to
            remove all relationships from org_1_id
        rel_type_id: the ID of nature of the relationship to remove
            (ignored if org_2_id is '*')
        source_id: the ID of the source for the removal information
        comment: a descriptive comment about the source of the removal

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_1_id, org_2_id, rel_type_id,
            or source_id is not valid
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
        db: an oria.DBConnection instance
        source_name: the source name to locate

    Returns:
        long: the source ID for the given source name

    Raises:
        DataSourceNonExistent: if an unknown source name is specified
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
        db: an oria.DBConnection instance
        scheme_name: the name of the scheme to locate

    Returns:
        long: the scheme ID for the given scheme name

    Raises:
        SchemeNonExistent: if an unknown scheme name is specified
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
        db: an oria.DBConnection instance

    Returns:
        list of str: the supported data sources
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
        db: an oria.DBConnection instance

    Returns:
        list of str: the supported scheme names
    """
    db.start()
    find_stmt = "SELECT name FROM master_other_id_scheme;"
    results = db.read_many(find_stmt, ())
    db.finish()

    scheme_names = [result[0] for result in results]
    return scheme_names

def merge_external_org( db, keep_id, lose_id, source_id,
                        comment=None ):
    """
    Adds all of the second organization’s properties to the first;
    expires the second organization and all its properties.  The
    source and comments from old assertions will be migrated; the
    source specified here will only be used for the expirations.

    There is a possibility of a race condition, as we copy all of the
    losing organization’s properties first, then expire the loser; it
    is hypothetically possible that a competing process could add
    properties to the loser in the interim.  This seems an acceptable
    risk at the current level of usage.

    Args:
        db: an oria.DBConnection instance
        keep_id: the ID of the external organization to merge into
        lose_id: the ID of the organization to merge and expire
        source_id: the ID of the source for these assertions
        comment: a descriptive comment about the source of these
            assertions

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if keep_id, lose_id, or source_id is
            not valid

    """
    # Copy aliases.
    db.start()
    alias_add_stmt = "INSERT IGNORE INTO " + \
                     "master_external_org_alias " + \
                     "( external_org, alias, lang, source, " + \
                     "source_comment ) " + \
                     "SELECT %s AS external_org, alias, lang, source, " + \
                     "source_comment " + \
                     "FROM master_external_org_alias " + \
                     "WHERE external_org = %s AND valid_end IS NULL;"
    alias_add_params = ( keep_id, lose_id )
    db.write( alias_add_stmt, alias_add_params )
    db.finish()

    # Copy other IDs.
    db.start()
    ids_add_stmt = "INSERT IGNORE INTO " + \
                   "master_external_org_other_id " + \
                   "( master_id, other_id, scheme, source, " + \
                   "source_comment ) " + \
                   "SELECT %s AS master_id, other_id, scheme, source, " + \
                   "source_comment " + \
                   "FROM master_external_org_other_id " + \
                   "WHERE master_id = %s AND valid_end IS NULL;"
    ids_add_params = ( keep_id, lose_id )
    db.write( ids_add_stmt, ids_add_params )
    db.finish()

    # Copy postcodes.
    db.start()
    post_add_stmt = "INSERT IGNORE INTO " + \
                    "master_external_org_postcode " + \
                    "( external_org, postcode, source, " + \
                    "source_comment ) " + \
                    "SELECT %s AS external_org, postcode, source, source_comment " + \
                    "FROM master_external_org_postcode " + \
                    "WHERE external_org = %s AND valid_end IS NULL;"
    post_add_params = ( keep_id, lose_id )
    db.write( post_add_stmt, post_add_params )
    db.finish()

    # Copy relationships.  This is a little tricky in that we need to
    # get the relationships in which the loser participates on either
    # end, *except* for ones that are between the winner and the
    # loser!  Those just expire.
    db.start()
    rel_add_1_stmt = "INSERT IGNORE INTO " + \
                     "master_rel_external_external " + \
                     "( ext1, ext2, rel, source, " + \
                     "source_comment ) " + \
                     "SELECT %s AS ext1, ext2, rel, source, " + \
                     "source_comment " + \
                     "FROM master_rel_external_external " + \
                     "WHERE ext1 = %s AND ext2 <> %s " + \
                     "AND valid_end IS NULL;"
    rel_add_1_params = ( keep_id, lose_id, keep_id )
    db.write( rel_add_1_stmt, rel_add_1_params )
    rel_add_2_stmt = "INSERT IGNORE INTO " + \
                     "master_rel_external_external " + \
                     "( ext1, ext2, rel, source, " + \
                     "source_comment ) " + \
                     "SELECT ext1, %s AS ext2, rel, source, " + \
                     "source_comment " + \
                     "FROM master_rel_external_external " + \
                     "WHERE ext2 = %s AND ext1 <> %s " + \
                     "AND valid_end IS NULL;"
    rel_add_2_params = ( keep_id, lose_id, keep_id )
    db.write( rel_add_2_stmt, rel_add_2_params )
    db.finish()

    # Delete the losing entity and all its properties.
    del_external_org( db, lose_id, source_id, comment )

    return

def rename_external_org( db, org_id, new_name, source_id,
                         comment=None, alias_old_name=True,
                         old_name_lang="en" ):
    """
    Give a new name to an external organization.

    This does not update the source or start date for the
    organization, though it will do so for the alias created, if any.

    For example, if an organization is found to have a Spanish name,
    it should be renamed as follows:
        rename_external_org( db, 42, "Generic University", 5,
                             comment="replacing Spanish names with English",
                             old_name_lang="es" )

    Will silently succeed if the organization already has that name.

    Args:
        db: an oria.DBConnection instance
        org_id: the ID of the target organization
        new_name: string giving the new English name of the
            organization
        source_id: the ID of the source for this assertion (only used
            if alias_old_name is True)
        comment: a descriptive comment about the source of this
            information (only used if alias_old_name is True)
        alias_old_name: True if the old name should be made an alias
            of the organization; False if the old name should be
            discarded
        old_name_lang: a valid ISO/IANA language code to use for
            creating an alias from the old name, if alias_old_name is
            True

    Returns:
        None

    Raises:
        MasterNonExistentEntity: if org_id is not valid, or if
            source_id is not valid and alias_old_name is True
    """
    db.start()
    find_stmt = "SELECT name FROM master_external_org " + \
                "WHERE id = %s;"
    ext_org_name = db.read( find_stmt, ( org_id, ) )
    db.finish()

    if ext_org_name is None:
        raise MasterNonExistentEntity(
            "There is no external organization with ID %d." % org_id
        )

    old_name = ext_org_name[0]

    update_stmt = "UPDATE master_external_org SET name = %s " + \
                  "WHERE id = %s;"
    _write_with_integrity( db, update_stmt, ( new_name, org_id ) )

    if alias_old_name:
        add_external_org_alias( db, org_id, old_name, source_id,
                                lang=old_name_lang, comment=comment )

    return

def get_master_id_for_other_id(db, other_id, scheme_id):
    """
    Get the master external_org ID for a given other_id and scheme_id.

    Args:
        db: The db instance
        other_id: The other id
        scheme_id: The scheme id

    Returns:
        str: The master external_org ID or None if none found
    """
    db.start()
    find_stmt = "SELECT master_id FROM master_external_org_other_id " + \
                "WHERE other_id = %s AND scheme = %s;"
    result = db.read(find_stmt, (other_id, scheme_id, ))
    db.finish()

    if result is not None:
        result = result[0]

    return result

def get_relationship_type_id(db, relationship):
    """
    Get the relationship identifier for a given relationship name.

    Args:
        db: The db instance
        relationship: The relationship name

    Returns:
        str: The relationship id for the given relationship name

    Raises:
        RelationshipNonExistent: If an unknown relationship is specified
    """
    db.start()
    find_stmt = "SELECT id FROM master_org_relationship_type " + \
                "WHERE name = %s;"
    result = db.read(find_stmt, (relationship, ))
    db.finish()

    if result is None:
        raise RelationshipNonExistent("Unknown relationship: %s" % relationship)

    relationship_id = result[0]
    return relationship_id

def get_supported_relationship_types(db):
    """
    Get the list of supported relationship types.

    Args:
        db: The db instance

    Returns:
        list of str: The supported relationship types
    """
    db.start()
    find_stmt = "SELECT name FROM master_org_relationship_type;"
    results = db.read_many(find_stmt, ())
    db.finish()

    relationship_types = [result[0] for result in results]
    return relationship_types
