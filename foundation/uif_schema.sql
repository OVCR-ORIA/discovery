DROP TABLE IF EXISTS uif_gift;
CREATE TABLE IF NOT EXISTS uif_gift (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  fiscal_year YEAR NOT NULL COMMENT 'The fiscal year in which this gift was received',
  gift_date DATE NOT NULL COMMENT 'The date on which this gift was received',
  gift_amount INTEGER NOT NULL COMMENT 'The amount in dollars credited for this gift',
  donor INTEGER(15) NOT NULL COMMENT 'Reference to the external organization making this gift',
  fund INTEGER(15) NOT NULL COMMENT 'Reference to the fund credited with this gift',
  purpose INTEGER(15) NOT NULL COMMENT 'Reference to the restriction category associated with this gift',
  gift_kind INTEGER(15) NOT NULL COMMENT 'Reference to the kind of instrument used to give this gift',
  gift_effort INTEGER(15) NOT NULL COMMENT 'Reference to the fundraising method resulting in this gift',
  PRIMARY KEY (id)
) COMMENT = 'Gifts as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_external_org;
CREATE TABLE IF NOT EXISTS uif_external_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  facts_id INTEGER(15) NOT NULL COMMENT 'FACTS ID for this external organization',
  name VARCHAR(255) NOT NULL COMMENT 'Name for an external organization, from UIF database',
  PRIMARY KEY (id),
  UNIQUE (facts_id)
) COMMENT = 'External organizations as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_fund;
CREATE TABLE IF NOT EXISTS uif_fund (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  fund_number INTEGER(15) NOT NULL COMMENT 'UIF internal tracking number for this fund',
  name VARCHAR(255) NOT NULL COMMENT 'Name for this fund, according to UIF',
  department INTEGER(15) NOT NULL COMMENT 'Reference to the department funded through this fund',
  PRIMARY KEY (id),
  UNIQUE (fund_number)
) COMMENT = 'University funds as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_department;
CREATE TABLE IF NOT EXISTS uif_department (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  department_code INTEGER(15) NOT NULL COMMENT 'UIF code for this department',
  banner_code VARCHAR(15) NULL COMMENT 'Banner code for this department, if known',
  description VARCHAR(255) NOT NULL COMMENT 'Name for this department, according to UIF',
  college INTEGER(15) NOT NULL COMMENT 'Reference to the college of which this department is part',
  PRIMARY KEY (id),
  UNIQUE (department_code)
) COMMENT = 'University departments as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_college;
CREATE TABLE IF NOT EXISTS uif_college (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  college_code INTEGER(15) NOT NULL COMMENT 'UIF code for this college',
  banner_code VARCHAR(15) NULL COMMENT 'Banner code for this college, if known',
  description VARCHAR(255) NOT NULL COMMENT 'Name for this college, according to UIF',
  campus INTEGER(15) NOT NULL COMMENT 'Reference to the campus where this college is located',
  PRIMARY KEY (id),
  UNIQUE (college_code)
) COMMENT = 'University colleges as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_campus;
CREATE TABLE IF NOT EXISTS uif_campus (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  campus_code INTEGER(15) NOT NULL COMMENT 'UIF code for this campus',
  abbreviation VARCHAR(15) NOT NULL COMMENT 'UIF abbreviation for this campus',
  description VARCHAR(255) NOT NULL COMMENT 'Name for this campus, according to UIF',
  PRIMARY KEY (id),
  UNIQUE (campus_code),
  UNIQUE (abbreviation)
) COMMENT = 'University campuses as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_gift_purpose;
CREATE TABLE IF NOT EXISTS uif_gift_purpose (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  purpose_code VARCHAR(15) NOT NULL COMMENT 'UIF code for this restriction',
  description VARCHAR(255) NOT NULL COMMENT 'Description of this restriction, according to UIF',
  PRIMARY KEY (id),
  UNIQUE (purpose_code)
) COMMENT = 'Gift restrictions (or lack) as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_gift_kind;
CREATE TABLE IF NOT EXISTS uif_gift_kind (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  kind_code VARCHAR(15) NOT NULL COMMENT 'UIF code for this kind of financial instrument or in-kind gift',
  description VARCHAR(255) NOT NULL COMMENT 'Description of this kind of gift, according to UIF',
  category INTEGER(15) NOT NULL COMMENT 'Reference to the general category in which this kind of gift fits',
  PRIMARY KEY (id),
  UNIQUE (kind_code)
) COMMENT = 'Specific instrumental nature of a gift as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_gift_kind_category;
CREATE TABLE IF NOT EXISTS uif_gift_kind_category (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  description VARCHAR(255) NOT NULL COMMENT 'Description of this category of gift kind, according to UIF',
  PRIMARY KEY (id),
  UNIQUE (description)
) COMMENT = 'General instrumental nature of a gift as tracked by the University of Illinois Foundation.';

DROP TABLE IF EXISTS uif_gift_effort;
CREATE TABLE IF NOT EXISTS uif_gift_effort (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  effort_code VARCHAR(15) NOT NULL COMMENT 'UIF code for this fundraising method',
  description VARCHAR(255) NOT NULL COMMENT 'Description of this fundraising method, according to UIF',
  PRIMARY KEY (id),
  UNIQUE (effort_code)
) COMMENT = 'Fundraising method as tracked by the University of Illinois Foundation.';

ALTER TABLE uif_gift
  ADD FOREIGN KEY fk_uif_gift_donor (donor) REFERENCES uif_external_org (id),
  ADD FOREIGN KEY fk_uif_gift_fund (fund) REFERENCES uif_fund (id),
  ADD FOREIGN KEY fk_uif_gift_purpose (purpose) REFERENCES uif_gift_purpose (id),
  ADD FOREIGN KEY fk_uif_gift_kind (gift_kind) REFERENCES uif_gift_kind (id),
  ADD FOREIGN KEY fk_uif_gift_effort (gift_effort) REFERENCES uif_gift_effort (id);

ALTER TABLE uif_fund
  ADD FOREIGN KEY fk_uif_fund_department (department) REFERENCES uif_department (id);

ALTER TABLE uif_department
  ADD FOREIGN KEY fk_uif_department_college (college) REFERENCES uif_college (id);

ALTER TABLE uif_college
  ADD FOREIGN KEY fk_uif_college_campus (campus) REFERENCES uif_campus (id);

ALTER TABLE uif_gift_kind
  ADD FOREIGN KEY fk_uif_gift_kind_category (category) REFERENCES uif_gift_kind_category (id);
