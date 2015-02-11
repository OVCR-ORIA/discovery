DROP TABLE IF EXISTS nih_institution_department;
CREATE TABLE IF NOT EXISTS nih_institution_department (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  dept_name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (dept_name)
) COMMENT = 'Institutional department, as tracked by NIH.  All within UIUC at present.';

DROP TABLE IF EXISTS nih_reviewer;
CREATE TABLE IF NOT EXISTS nih_reviewer (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  spriden_pidm DECIMAL(8,0) NULL COMMENT 'Faculty PIDM, if known',
  last_name VARCHAR(255) NOT NULL,
  first_name VARCHAR(255) NOT NULL,
  middle_name VARCHAR(255) NULL,
  title VARCHAR(255) NULL COMMENT 'Title as recorded by NIH',
  department INTEGER(15) NULL COMMENT 'Reference to institutional department as recorded by NIH',
  service_months INTEGER(15) NOT NULL COMMENT 'Accountable committee service length, in months',
  PRIMARY KEY (id),
  FOREIGN KEY reviewer_pidm (spriden_pidm) REFERENCES spriden_norm (spriden_pidm),
  FOREIGN KEY reviewer_department (department) REFERENCES nih_institution_department (id)
) COMMENT = 'Faculty members who participated in NIH study groups.';

DROP TABLE IF EXISTS nih_study_section;
CREATE TABLE IF NOT EXISTS nih_study_section (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'NIH study sections by name.';

DROP TABLE IF EXISTS nih_study_section_role;
CREATE TABLE IF NOT EXISTS nih_study_section_role (
  code VARCHAR(15) NOT NULL,
  description VARCHAR(255) NULL,
  PRIMARY KEY (code)
) COMMENT = 'NIH study section roles by code and name.';

DROP TABLE IF EXISTS nih_study_section_participation;
CREATE TABLE IF NOT EXISTS nih_study_section_participation (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  reviewer INTEGER(15) NOT NULL COMMENT 'Reference to the faculy member participating in the study section',
  study_section INTEGER(15) NOT NULL COMMENT 'Reference to the study section participated in',
  role VARCHAR(15) NOT NULL COMMENT 'Reference to the study section role',
  start_date DATE NOT NULL COMMENT 'Beginning of a particular period of study section participation',
  end_date DATE NOT NULL COMMENT 'End of a particular period of study section participation',
  PRIMARY KEY (id),
  FOREIGN KEY participation_faculty (reviewer) REFERENCES nih_reviewer (id),
  FOREIGN KEY participation_section (study_section) REFERENCES nih_study_section (id),
  FOREIGN KEY participation_role (role) REFERENCES nih_study_section_role (code)
) COMMENT = 'Instances of participation in a study group by a faculty member.';
