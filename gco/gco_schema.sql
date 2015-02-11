DROP TABLE IF EXISTS gco_grant;
CREATE TABLE IF NOT EXISTS gco_grant (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  banner_id VARCHAR(15) NOT NULL COMMENT 'Banner code for the grant',
  title VARCHAR(255) NOT NULL COMMENT 'Short title of the grant',
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  grant_type INTEGER(15) NOT NULL COMMENT 'Reference to GCO grant type classification',
  grant_category INTEGER(15) NOT NULL COMMENT 'Reference to GCO grant categorization',
  investigator INTEGER(15) NOT NULL COMMENT 'Reference to responsible University primary investigator',
  responsible_org INTEGER(15) NOT NULL COMMENT 'Reference to the internal organization responsible for the grant',
  sponsor INTEGER(15) NOT NULL COMMENT 'Reference to the external organization sponsoring the grant',
  passthrough_sponsor INTEGER(15) COMMENT 'Optional reference to the external organization providing pass-through funding',
  long_title TEXT COMMENT 'Full title of the grant',
  PRIMARY KEY (id),
  UNIQUE (banner_id)
) COMMENT = 'Grants as tracked by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_grant_year;
CREATE TABLE IF NOT EXISTS gco_grant_year (
  grant_id INTEGER(15) NOT NULL COMMENT 'Reference to the grant being tracked',
  fiscal_year YEAR NOT NULL COMMENT 'The fiscal year for which this grant is being tracked',
  budget DECIMAL(15,2) NOT NULL COMMENT 'Year-to-date budget for this grant',
  expenditures DECIMAL(15,2) NOT NULL COMMENT 'Year-to-date total expenditures for this grant (including F&A)',
  overhead DECIMAL(15,2) NOT NULL COMMENT 'Year-to-date F&A expenditures for this grant',
  PRIMARY KEY (grant_id, fiscal_year)
) COMMENT = 'Financial information about a grant for a single fiscal year, as tracked by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_grant_type;
CREATE TABLE IF NOT EXISTS gco_grant_type (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  type_code VARCHAR(15) NOT NULL COMMENT 'GCO code for this type',
  description VARCHAR(255) NOT NULL COMMENT 'Short description of this type, according to GCO',
  PRIMARY KEY (id),
  UNIQUE (type_code)
) COMMENT = 'Grant types, as classified by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_grant_category;
CREATE TABLE IF NOT EXISTS gco_grant_category (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  category_code VARCHAR(15) NOT NULL COMMENT 'GCO code for this category',
  description VARCHAR(255) NOT NULL COMMENT 'Short description of this category, according to the GCO',
  PRIMARY KEY (id),
  UNIQUE (category_code)
) COMMENT = 'Grant (sub)categories, as classified by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_investigator;
CREATE TABLE IF NOT EXISTS gco_investigator (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  uin VARCHAR(15) NOT NULL COMMENT 'University-issued UIN of investigator',
  last_name VARCHAR(63) NOT NULL,
  first_name VARCHAR(63) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (uin)
) COMMENT = 'Investigators as tracked by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_internal_org;
CREATE TABLE IF NOT EXISTS gco_internal_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  org_code VARCHAR(15) NOT NULL COMMENT 'GCO organizational code for a campus unit',
  description VARCHAR(255) NOT NULL COMMENT 'Short description or name of a campus unit according to GCO',
  PRIMARY KEY (id),
  UNIQUE (org_code)
) COMMENT = 'Internal University organizations as tracked by the Office of Grants and Contracts.';

DROP TABLE IF EXISTS gco_external_org;
CREATE TABLE IF NOT EXISTS gco_external_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  banner_id VARCHAR(15) NOT NULL COMMENT 'Banner code for an external organization',
  name VARCHAR(255) NOT NULL COMMENT 'Name for an external organization, from Banner',
  category_name VARCHAR(255) COMMENT 'Summary name of the appropriate Banner hierarchy level for this organization',
  l1_category_name VARCHAR(255) COMMENT 'Name of the top-level Banner hierarchy for this organization',
  l2_category_name VARCHAR(255) COMMENT 'Name of the second-level Banner hierarchy for this organization, if any',
  l3_category_name VARCHAR(255) COMMENT 'Name of the third-level Banner hierarchy for this organization, if any',
  us_commercial BOOLEAN COMMENT 'True if the organization is a US corporate or commercial organization',
  foreign_commercial BOOLEAN COMMENT 'True if the organization is a non-US corporate or commercial organization',
  PRIMARY KEY (id),
  UNIQUE (banner_id)
) COMMENT = 'External organizations as tracked by the Office of Grants and Contracts.';

ALTER TABLE gco_grant
  ADD FOREIGN KEY fk_gco_grant_type (grant_type) REFERENCES gco_grant_type (id),
  ADD FOREIGN KEY fk_gco_grant_category (grant_category) REFERENCES gco_grant_category (id),
  ADD FOREIGN KEY fk_gco_grant_investigator (investigator) REFERENCES gco_investigator (id),
  ADD FOREIGN KEY fk_gco_grant_responsible (responsible_org) REFERENCES gco_internal_org (id),
  ADD FOREIGN KEY fk_gco_grant_sponsor (sponsor) REFERENCES gco_external_org (id),
  ADD FOREIGN KEY fk_gco_grant_passthrough_sponsor (passthrough_sponsor) REFERENCES gco_external_org (id);

ALTER TABLE gco_grant_year
  ADD FOREIGN KEY fk_gco_grant_year_grant (grant_id) REFERENCES gco_grant (id);
