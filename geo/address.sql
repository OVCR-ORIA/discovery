DROP TABLE IF EXISTS address;
CREATE TABLE IF NOT EXISTS address (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  street1 VARCHAR(255) NULL,
  street2 VARCHAR(255) NULL,
  city VARCHAR(255) NULL,
  state_province VARCHAR(255) NULL,
  postcode VARCHAR(15) NULL,
  nation VARCHAR(15) NULL,
  addr_string VARCHAR(1023) NULL COMMENT 'Raw string representation of address',
  addr_string_norm VARCHAR(1023) NULL COMMENT 'Normalized string representation of address, from geocoding service',
  state_province_ref INTEGER(15) NULL COMMENT 'Reference to first-level country division, if known',
  postcode_ref INTEGER(15) NULL COMMENT 'Reference to postcode, if known',
  nation_ref INTEGER(15) NULL COMMENT 'Reference to country, if known',
  location POINT NULL COMMENT 'Geocoded latitude and longitude of the address',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (id),
  UNIQUE (addr_string),
  FOREIGN KEY fk_address_state_province (state_province_ref) REFERENCES country_div_1 (id),
  FOREIGN KEY fk_address_postcode (postcode_ref) REFERENCES postcode (id),
  FOREIGN KEY fk_address_nation (nation_ref) REFERENCES country (id),
  FOREIGN KEY fk_address_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Entity master for geocoded physical addresses.';

DROP TABLE IF EXISTS address_congressional_district;
CREATE TABLE IF NOT EXISTS address_congressional_district (
  address INTEGER(15) NOT NULL COMMENT 'Reference to the address in this district',
  congressional_district INTEGER(15) NOT NULL COMMENT 'Reference to the district in which this address is found',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (address, congressional_district),
  FOREIGN KEY fk_address_congress_address (address) REFERENCES address (id),
  FOREIGN KEY fk_address_congress_district (congressional_district) REFERENCES congressional_district (id),
  FOREIGN KEY fk_address_congress_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Correlation between addresses and US Congressional districts.';
