DROP TABLE IF EXISTS master_other_id_scheme;
CREATE TABLE IF NOT EXISTS master_other_id_scheme (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(15) NOT NULL COMMENT 'Human-readable label for correlated ID scheme',
  comment VARCHAR(255) NULL COMMENT 'Additional description of correlated ID scheme',
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'Controlled list of other ID schemes to correlate with mastered entities.';

DROP TABLE IF EXISTS master_external_org_other_id;
CREATE TABLE IF NOT EXISTS master_external_org_other_id (
  master_id INTEGER(15) NOT NULL COMMENT 'Reference to an external organization in our entity master',
  other_id VARCHAR(15) NOT NULL COMMENT 'Reference to an ID in some other scheme',
  scheme INTEGER(15) NOT NULL COMMENT 'Reference to the other scheme being used',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  UNIQUE (master_id, other_id, scheme),
  FOREIGN KEY fk_master_external_org_other_id_master (master_id) REFERENCES master_external_org (id),
  FOREIGN KEY fk_master_external_org_other_id_scheme (scheme) REFERENCES master_other_id_scheme (id),
  FOREIGN KEY fk_master_external_org_other_id_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Correlates mastered external entities with IDs from other schemes.';
