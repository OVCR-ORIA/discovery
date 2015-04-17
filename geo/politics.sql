DROP TABLE IF EXISTS congressional_district;
CREATE TABLE IF NOT EXISTS congressional_district 
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  state INTEGER(15) NOT NULL COMMENT 'Reference to US state',
  district_number INTEGER(2) NOT NULL COMMENT 'District number, within the state',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (id),
  UNIQUE (state, district_number),
  FOREIGN KEY fk_congressional_district_state (state) REFERENCES country_div_1 (id),
  FOREIGN KEY fk_congressional_district_source (source) REFERENCES master_data_source (id)
) COMMENT = 'USA Congressional districts.';
