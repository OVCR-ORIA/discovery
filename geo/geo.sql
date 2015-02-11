DROP TABLE IF EXISTS country;
CREATE TABLE IF NOT EXISTS country (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  iso3166 VARCHAR(2) CHARACTER SET latin1 NOT NULL COMMENT 'ISO 3166 country code',
  name VARCHAR(63) CHARACTER SET latin1 NOT NULL COMMENT 'English common name of this country',
  PRIMARY KEY (id),
  UNIQUE (iso3166)
) COMMENT = 'List of countries for geo data.'

DROP TABLE IF EXISTS country_div_1;
CREATE TABLE IF NOT EXISTS country_div_1 (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  country INTEGER(15) NOT NULL COMMENT 'Reference to the country of which this is a division',
  code VARCHAR(20) NOT NULL COMMENT 'Administrative code of this division (e.g. US state abbreviation)',
  name VARCHAR(127) NULL COMMENT 'Name of this division; may be blank',
  PRIMARY KEY (id),
  UNIQUE (country, code),
  FOREIGN KEY fk_country_div_1_country (country) REFERENCES country (id)
) COMMENT = 'Top-level country divisions (e.g. US states).'

DROP TABLE IF EXISTS country_div_2;
CREATE TABLE IF NOT EXISTS country_div_2 (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  div_1 INTEGER(15) NOT NULL COMMENT 'Reference to the first-level division of which this is a smaller division',
  code VARCHAR(20) NOT NULL COMMENT 'Administrative code of this division (e.g., FIPS code of a U.S. county)',
  name VARCHAR(127) NULL COMMENT 'Name of this division; may be blank',
  PRIMARY KEY (id),
  UNIQUE (div_1, code),
  FOREIGN KEY fk_country_div_2_div_1 (div_1) REFERENCES country_div_1 (id)
) COMMENT = 'Second-level country divisions (e.g. US counties).'

DROP TABLE IF EXISTS country_div_3;
CREATE TABLE IF NOT EXISTS country_div_3 (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  div_2 INTEGER(15) NOT NULL COMMENT 'Reference to the second-level division of which this is a smaller division',
  code VARCHAR(20) NOT NULL COMMENT 'Administrative code of this division',
  name VARCHAR(127) NULL COMMENT 'Name of this division; may be blank',
  PRIMARY KEY (id),
  UNIQUE (div_2, code),
  FOREIGN KEY fk_country_div_3_div_2 (div_2) REFERENCES country_div_2 (id)
) COMMENT = 'Third-level country divisions (not used in U.S.).'

DROP TABLE IF EXISTS postcode;
CREATE TABLE IF NOT EXISTS postcode (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  postcode VARCHAR(20) NOT NULL COMMENT 'Postal code (e.g., US ZIP code)',
  name VARCHAR(255) NOT NULL COMMENT 'Name of the place associated with the postal code',
  div_1 INTEGER(15) NOT NULL COMMENT 'Reference to the first-level country division in which this postal code is located',
  div_2 INTEGER(15) NULL COMMENT 'Reference to the second-level country division in which this postal code is located, if known',
  div_3 INTEGER(15) NULL COMMENT 'Reference to the third-level country division in which this postal code is located, if known',
  location point NULL COMMENT 'Latitude and longitude of the centroid of the postal code, if known, in WGS84',
  PRIMARY KEY (id),
  FOREIGN KEY postcode_div_1 (div_1) REFERENCES country_div_1 (id),
  FOREIGN KEY postcode_div_2 (div_2) REFERENCES country_div_2 (id),
  FOREIGN KEY postcode_div_3 (div_3) REFERENCES country_div_3 (id)
) COMMENT = 'Postal codes by country division (e.g. US ZIP codes).'
