#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Load grant award data from the National Science Foundation into the
ORIA database.

Input data is XML as described at
<URL: http://www.nsf.gov/awardsearch/download.jsp > (but note that
documents do not comply with the given schema).

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"18 September 2014" # Alba gu bràth!
__version__ = 1.0

# Adjust the load path for common data loading operations.
import sys
sys.path.append( '../lib' )

# Common data loading tools.
import oria

# XML parsing and manipulation.
from lxml import etree

# Date mangling.
from datetime import datetime

def exactly_one( award, name, elts ):
    """
    Check that there is exactly one element in elts.  Return True if
    it passes; return False and report an error otherwise.
    """
    if len( elts ) < 1:
        sys.stderr.write( "Award %s has no %s!\n" %
                          ( award_id, name ) )
        return False
    if len( elts ) > 1:
        sys.stderr.write( "Award %s has more than one %s; " %
                          ( award_id, name ) +
                          "using the first one.\n" )
        return False

    return True

def get_date( date_str ):
    """
    NSF dates are all formated mm/dd/yyyy; convert those into proper
    date objects, or None if the string is empty.
    """
    if date_str is None or date_str.strip() == "":
        return None
    else:
        return datetime.strptime( date_str, "%m/%d/%Y" ).date()

def get_int( num_str ):
    """
    Some elements represent numeric values, but may be blank or empty.
    Return an integer, or None if the string is empty.
    """
    if num_str is None or num_str.strip() == "":
        return None
    else:
        return int( num_str )

def get_str( str_str ):
    """
    When a text-valued element is blank or empty, return None instead
    of loading empty strings into the database.
    """
    if str_str is None:
        return None

    str_strip = str_str.strip()
    if str_strip == "":
        return None

    return str_strip

def main():
    """
    Read in a series of command-line specified XML files and load them
    into the database.
    """
    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads XML NSF data into oria DB"
    )
    parser.add_argument( "file", nargs="+", type=str,
                         help="XML file(s) to read" )
    args = parser.parse_args()

    # Set up some caching for shared entities.  This makes offline
    # testing much easier.  It has the possibility of a race
    # condition, but we really should be the only entity writing to
    # these tables anyway.
    foas = {}
    instruments = {}
    nsf_orgs = {}
    progs = {}
    roles = {}
    next_inst = 1
    next_inv = 1

    # Connect to the database.
    db = oria.DBConnection( offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Read each file.
    for xmlfilename in args.file:
        xmlfile = open( xmlfilename )
        nsftree = etree.parse( xmlfile )
        xmlfile.close()

        award_id = nsftree.find( "//AwardID" ).text.strip()

        # Find or create the instrument.
        instr_elts = nsftree.findall( "//AwardInstrument/Value" )
        exactly_one( award_id, "instrument", instr_elts )
        instr_name = get_str( instr_elts[0].text )

        instr_id = db.get_or_set_id( instruments,
                                     "nsf_award_instrument",
                                     "name",
                                     { "name" : instr_name } )

        # Find or create the NSF internal organization.
        org_elts = nsftree.findall( "//Organization" )
        exactly_one( award_id, "NSF internal org", org_elts )
        nsf_org = org_elts[0]
        nsf_org_code = get_str( nsf_org.find( "Code" ).text )
        nsf_org_dir = \
            get_str( nsf_org.find( "Directorate/LongName" ).text )
        nsf_org_div = \
            get_str( nsf_org.find( "Division/LongName" ).text )

        nsf_org_id = db.get_or_set_id( nsf_orgs,
                                       "nsf_internal_org",
                                       "code",
                                       { "code" : nsf_org_code,
                                         "directorate" : nsf_org_dir,
                                         "division" : nsf_org_div } )

        # Create the award.
        title = get_str( nsftree.find( "//AwardTitle" ).text )
        date_effective = \
            get_date( nsftree.find( "//AwardEffectiveDate" ).text )
        date_expires = \
            get_date( nsftree.find( "//AwardExpirationDate" ).text )
        amount = get_int( nsftree.find( "//AwardAmount" ).text )
        officer = \
            get_str( nsftree.find(
                "//ProgramOfficer/SignBlockName" ).text )
        abstract = \
            get_str( nsftree.find( "//AbstractNarration" ).text )
        min_amd = \
            get_date( nsftree.find( "//MinAmdLetterDate" ).text )
        max_amd = \
            get_date( nsftree.find( "//MaxAmdLetterDate" ).text )
        arra_amount = get_int( nsftree.find( "//ARRAAmount" ).text )
        if arra_amount is None:
            arra_amount = 0

        db.get_or_set_id( {},
                          "nsf_award",
                          "id",
                          { "id" : award_id,
                            "title" : title,
                            "date_effective" : date_effective,
                            "date_expires" : date_expires,
                            "amount" : amount,
                            "instrument" : instr_id,
                            "nsf_organization" : nsf_org_id,
                            "program_officer" : officer,
                            "abstract" : abstract,
                            "min_amd_letter_date" : min_amd,
                            "max_amd_letter_date" : max_amd,
                            "arra_amount" : arra_amount } )

        # Find or create the investigators and their roles and connect
        # them to the award.
        investigator_elts = nsftree.findall( "//Investigator" )
        for investigator in investigator_elts:
            inv_first = \
                get_str( investigator.find( "FirstName" ).text )
            inv_last = \
                get_str( investigator.find( "LastName" ).text )
            inv_email = \
                get_str( investigator.find( "EmailAddress" ).text )
            inv_start = \
                get_date( investigator.find( "StartDate" ).text )
            inv_end = \
                get_date( investigator.find( "EndDate" ).text )
            inv_role = \
                get_str( investigator.find( "RoleCode" ).text )

            role_id = db.get_or_set_id( roles,
                                        "nsf_investigator_role",
                                        "name",
                                        { "name" : inv_role } )

            # We consider investigators identical iff they share first
            # and last names and e-mail address.
            inv_id = None
            if inv_email is not None:
                inv_read_stmt = \
                    "SELECT id, first_name, last_name " + \
                    "FROM nsf_investigator WHERE email = %s;"
                inv_cands = db.read_many( inv_read_stmt,
                                          ( inv_email, ) )
                for inv_cand in inv_cands:
                    cand_id, cand_first, cand_last = inv_cand
                    if inv_first is not None and \
                            cand_first.strip() == inv_first and \
                            inv_last is not None and \
                            cand_last.strip() == inv_last:
                        inv_id = cand_id
                        break

            db.start()
            if inv_id is None:
                inv_write_stmt = \
                    "INSERT INTO nsf_investigator " + \
                    "( first_name, last_name, email ) " + \
                    "VALUES ( %s, %s, %s );"
                db.write( inv_write_stmt,
                          ( inv_first, inv_last, inv_email ) )
                if args.offline or args.test:
                    inv_id = next_inv
                    next_inv += 1
                else:
                    inv_id = db.read( "SELECT LAST_INSERT_ID()",
                                      () )[0]

            inv_award_stmt = \
                "INSERT IGNORE INTO nsf_award_investigator " + \
                "( award, investigator, role, date_start, " + \
                "date_end ) " + \
                "VALUES ( %s, %s, %s, %s, %s );"
            db.write( inv_award_stmt,
                      ( award_id, inv_id, role_id, inv_start,
                        inv_end ) )
            db.finish()

        # Find or create the institutions and connect them to the
        # award.
        inst_elts = nsftree.findall( "//Institution" )
        for inst in inst_elts:
            inst_name = get_str( inst.find( "Name" ).text )
            inst_street = get_str( inst.find( "StreetAddress" ).text )
            inst_city = get_str( inst.find( "CityName" ).text )
            inst_state = get_str( inst.find( "StateName" ).text )
            inst_st_cd = get_str( inst.find( "StateCode" ).text )
            inst_zip = get_str( inst.find( "ZipCode" ).text )
            inst_country = get_str( inst.find( "CountryName" ).text )
            inst_phone = get_str( inst.find( "PhoneNumber" ).text )

            # We consider institutions identical if they share a name
            # and state.
            db.start()

            inst_id = None

            if inst_st_cd is not None:
                inst_read_stmt = \
                    "SELECT id FROM nsf_external_org " + \
                    "WHERE name = %s AND state_code = %s;"
                inst_cand = db.read( inst_read_stmt,
                                     ( inst_name, inst_st_cd ) )
                if inst_cand is not None:
                    inst_id = inst_cand[0]

            if inst_id is None:
                inst_write_stmt = \
                    "INSERT INTO nsf_external_org " + \
                    "( name, street, city, state, state_code, " + \
                    "zip, country, phone ) " + \
                    "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s );"
                db.write( inst_write_stmt,
                          ( inst_name, inst_street, inst_city,
                            inst_state, inst_st_cd, inst_zip,
                            inst_country, inst_phone ) )
                if args.offline or args.test:
                    inst_id = next_inst
                    next_inst += 1
                else:
                    inst_id = db.read( "SELECT LAST_INSERT_ID()",
                                       () )[0]

            inst_award_stmt = \
                "INSERT IGNORE INTO nsf_award_institution " + \
                "( award, institution ) " + \
                "VALUES ( %s, %s );"
            db.write( inst_award_stmt, ( award_id, inst_id ) )
            db.finish()

        # Find or create the FOAs and connect them to the award.
        foa_elts = nsftree.findall( "//FoaInformation" )
        for foa in foa_elts:
            foa_code = get_str( foa.find( "Code" ).text )
            foa_name = get_str( foa.find( "Name" ).text )

            db.get_or_set_id( foas,
                              "nsf_funding_opportunity",
                              "code",
                              { "code" : foa_code,
                                "name" : foa_name },
                              id="code" )

            db.start()
            foa_award_stmt = \
                "INSERT IGNORE INTO nsf_award_foa ( award, foa ) " + \
                "VALUES ( %s, %s );"
            db.write( foa_award_stmt, ( award_id, foa_code ) )
            db.finish()

        # Find or create the NSF programs and connect them to the
        # award.
        prog_elts = nsftree.findall( "//ProgramElement" )
        prog_refs = nsftree.findall( "//ProgramReference" )
        for prog in prog_elts + prog_refs:
            prog_code = get_str( prog.find( "Code" ).text )
            if prog_code is None:
                continue
            prog_name = get_str( prog.find( "Text" ).text )
            prog_element = prog.tag == "ProgramElement"

            db.get_or_set_id( progs,
                              "nsf_program",
                              "code",
                              { "code" : prog_code,
                                "name" : prog_name },
                              id="code" )

            db.start()
            prog_award_stmt = \
                "INSERT IGNORE INTO nsf_award_program " + \
                "( award, program, is_element ) " + \
                "VALUES ( %s, %s, %s );"
            db.write( prog_award_stmt,
                      ( award_id, prog_code, prog_element ) )
            db.finish()

    return

if __name__ == '__main__':
    main()
    exit( 0 )
