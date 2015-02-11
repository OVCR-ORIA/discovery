DROP TABLE IF EXISTS nsf_award;
CREATE TABLE IF NOT EXISTS nsf_award (
  id VARCHAR(15) NOT NULL COMMENT 'NSF-assigned ID for a grant award',
  title VARCHAR(1023) NOT NULL COMMENT 'Title of the awarded grant',
  date_effective DATE NOT NULL COMMENT 'Date on which the award is effective',
  date_expires DATE NOT NULL COMMENT 'Date on which the award ends',
  amount INTEGER(15) NOT NULL COMMENT 'Amount, in whole US dollars, of the award',
  instrument INTEGER(15) NOT NULL COMMENT 'Reference to the nature of the award financial process',
  nsf_organization INTEGER(15) NOT NULL COMMENT 'Reference to the NSF internal body responsible for this award',
  program_officer VARCHAR(255) NULL COMMENT 'Name of the NSF person responsible for the grant, if known',
  abstract TEXT NULL COMMENT 'Abstract narration from the grant proposal',
  min_amd_letter_date DATE NULL COMMENT '?',
  max_amd_letter_date DATE NULL COMMENT '?',
  arra_amount INTEGER(15) NOT NULL DEFAULT 0 COMMENT 'Amount of the grant award pursuant to the American Recovery and Reinvestment Act of 2009',
  PRIMARY KEY(id)
) COMMENT = 'National Science Foundation grant awards, as reported by NSF.';

DROP TABLE IF EXISTS nsf_award_instrument;
CREATE TABLE IF NOT EXISTS nsf_award_instrument (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL COMMENT 'Name of the instrument',
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'Financial methods used to pay NSF grant awards.';

DROP TABLE IF EXISTS nsf_internal_org;
CREATE TABLE IF NOT EXISTS nsf_internal_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  code VARCHAR(15) NOT NULL COMMENT 'The NSF code for this organization',
  directorate VARCHAR(255) NULL COMMENT 'The name of the NSF directorate, if any, responsible for this award',
  division VARCHAR(255) NULL COMMENT 'The name of the NSF division, if any, responsible for this award',
  PRIMARY KEY (id),
  UNIQUE (code)
) COMMENT = 'Internal organizational structures of the NSF.';

DROP TABLE IF EXISTS nsf_investigator;
CREATE TABLE IF NOT EXISTS nsf_investigator (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(255) NULL,
  last_name VARCHAR(255) NULL,
  email VARCHAR(255) NULL,
  PRIMARY KEY (id)
) COMMENT = 'Investigators known to the NSF, attached to one or more awards.';

DROP TABLE IF EXISTS nsf_investigator_role;
CREATE TABLE IF NOT EXISTS nsf_investigator_role (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'A controlled vocabulary of roles that an investigator might have with respect to a specific award.';

DROP TABLE IF EXISTS nsf_award_investigator;
CREATE TABLE IF NOT EXISTS nsf_award_investigator (
  award VARCHAR(15) NOT NULL COMMENT 'Reference to the award in question',
  investigator INTEGER(15) NOT NULL COMMENT 'Reference to the investigator in question',
  role INTEGER(15) NOT NULL COMMENT 'Reference to the role the investigator played on this award',
  date_start DATE NOT NULL COMMENT 'Date on which the investigator’s association began',
  date_end DATE NULL COMMENT 'Date on which the investigator’s association ended, if known',
  PRIMARY KEY (award, investigator),
  FOREIGN KEY fk_nsf_investigator_award (award) REFERENCES nsf_award (id),
  FOREIGN KEY fk_nsf_award_investigator (investigator) REFERENCES nsf_investigator (id),
  FOREIGN KEY fk_nsf_investigator_role (role) REFERENCES nsf_investigator_role (id)
) COMMENT = 'Associations of an investigator with an awarded grant.';

DROP TABLE IF EXISTS nsf_external_org;
CREATE TABLE IF NOT EXISTS nsf_external_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL COMMENT 'Name of the organization, as known to NSF',
  street VARCHAR(255) NULL COMMENT 'Street address of the organization, if known',
  city VARCHAR(255) NULL COMMENT 'City of address of the organization, if known',
  state VARCHAR(255) NULL COMMENT 'US state of the organization, if appropriate and known',
  state_code VARCHAR(15) NULL COMMENT 'Abbreviation of the US state of the organization, if appropriate and known',
  zip VARCHAR(15) NULL COMMENT 'The 9-digit US ZIP code of the organization, if appropriate and known',
  country VARCHAR(15) NULL COMMENT 'The nation in which the organization is located, if known',
  phone VARCHAR(15) NULL COMMENT 'The NANP contact phone number of the organization, if known',
  master_id INTEGER(15) NULL COMMENT 'Reference to the external organization entity master for this organization',
  PRIMARY KEY (id),
  FOREIGN KEY fk_nsf_external_org_master (master_id) REFERENCES master_external_org (id)
) COMMENT = 'Organizations external to NSF, as known to NSF.';

DROP TABLE IF EXISTS nsf_award_institution;
CREATE TABLE IF NOT EXISTS nsf_award_institution (
  award VARCHAR(15) NOT NULL COMMENT 'Reference to the award in question',
  institution INTEGER(15) NOT NULL COMMENT 'Reference to the associated institution',
  PRIMARY KEY (award, institution),
  FOREIGN KEY fk_nsf_institution_award (award) REFERENCES nsf_award (id),
  FOREIGN KEY fk_nsf_award_institution (institution) REFERENCES nsf_external_org (id)
) COMMENT = 'Associations of a recipient institution with an awarded grant.';

DROP TABLE IF EXISTS nsf_funding_opportunity;
CREATE TABLE IF NOT EXISTS nsf_funding_opportunity (
  code VARCHAR(15) NOT NULL COMMENT 'NSF code for a funding opportunity announcement',
  name VARCHAR(255) NOT NULL COMMENT 'The name of this particular funding opportunity',
  PRIMARY KEY (code)
) COMMENT = 'Specific announcements of funding opportunities (FOAs) at NSF.';

DROP TABLE IF EXISTS nsf_award_foa;
CREATE TABLE IF NOT EXISTS nsf_award_foa (
  award VARCHAR(15) NOT NULL COMMENT 'Reference to the award in question',
  foa VARCHAR(15) NOT NULL COMMENT 'Reference to the associated FOA',
  PRIMARY KEY (award, foa),
  FOREIGN KEY fk_nsf_foa_award (award) REFERENCES nsf_award (id),
  FOREIGN KEY fk_nsf_award_foa (foa) REFERENCES nsf_funding_opportunity (code)
) COMMENT = 'Associations of grant awards with funding opportunity announcements.';

DROP TABLE IF EXISTS nsf_program;
CREATE TABLE IF NOT EXISTS nsf_program (
  code VARCHAR(15) NOT NULL COMMENT 'NSF code for the program',
  name VARCHAR(255) NULL COMMENT 'Name of this program',
  PRIMARY KEY (code)
) COMMENT = 'NSF internal programs.';

DROP TABLE IF EXISTS nsf_award_program;
CREATE TABLE IF NOT EXISTS nsf_award_program (
  award VARCHAR(15) NOT NULL COMMENT 'Reference to the award in question',
  program VARCHAR(15) NOT NULL COMMENT 'Reference to the associated program',
  is_element BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'If true, the award is an element of this program; if false, there is a reference to the program',
  PRIMARY KEY (award, program),
  FOREIGN KEY fk_nsf_program_award (award) REFERENCES nsf_award (id),
  FOREIGN KEY fk_nsf_award_program (program) REFERENCES nsf_program (code)
) COMMENT = 'Associations of awards with NSF programs.';

ALTER TABLE nsf_award
  ADD FOREIGN KEY fk_nsf_award_instrument (instrument) REFERENCES nsf_award_instrument (id),
  ADD FOREIGN KEY fk_nsf_awarding_organization (nsf_organization) REFERENCES nsf_internal_org (id);
