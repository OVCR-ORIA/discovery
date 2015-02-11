DROP TABLE IF EXISTS master_external_org;
CREATE TABLE IF NOT EXISTS master_external_org (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL COMMENT 'The best available normative English name for this organization',
  educational BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Indicates that this is an educational institution',
  business BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Indicates that this is a for-profit business',
  nonprofit BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Indicates that this is a non-profit (but non-governmental) organization',
  government BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Indicates that this is a government body',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (id),
  FOREIGN KEY fk_master_external_org_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Entity master for external organizations.';

DROP TABLE IF EXISTS master_external_org_alias;
CREATE TABLE IF NOT EXISTS master_external_org_alias (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  external_org INTEGER(15) NOT NULL COMMENT 'Reference to the organization being named',
  alias VARCHAR(255) NOT NULL COMMENT 'The name for the organization',
  lang VARCHAR(15) NULL COMMENT 'The ISO/IANA language code for the name, if known',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (id),
  UNIQUE (external_org, alias, lang),
  FOREIGN KEY fk_master_external_org_alias (external_org) REFERENCES master_external_org (id),
  FOREIGN KEY fk_master_external_org_alias_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Provides additional names for external organizations.';

DROP TABLE IF EXISTS master_org_relationship_type;
CREATE TABLE IF NOT EXISTS master_org_relationship_type (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(15) NOT NULL COMMENT 'Human-readable label for inter-organizational relationship',
  forward VARCHAR(255) NOT NULL COMMENT 'connecting language from entity 1 to entity 2',
  inverse VARCHAR(255) NOT NULL COMMENT 'connecting language from entity 2 to entity 1',
  comment VARCHAR(255) NULL COMMENT 'Additional description of inter-organizational relationship',
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'Controlled list of inter-organizational relationships.';

DROP TABLE IF EXISTS master_rel_external_external;
CREATE TABLE IF NOT EXISTS master_rel_external_external (
  ext1 INTEGER(15) NOT NULL COMMENT 'Reference to the first organization in the relationship',
  ext2 INTEGER(15) NOT NULL COMMENT 'Reference to the second organization in the relationship',
  rel INTEGER(15) NOT NULL COMMENT 'The nature of the relationship',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (ext1, ext2, rel),
  FOREIGN KEY fk_master_rel_ext_ext_ext1 (ext1) REFERENCES master_external_org (id),
  FOREIGN KEY fk_master_rel_ext_ext_ext2 (ext2) REFERENCES master_external_org (id),
  FOREIGN KEY fk_master_rel_ext_ext_rel (rel) REFERENCES master_org_relationship_type (id),
  FOREIGN KEY fk_master_external_external_source (source) REFERENCES master_data_source (id)
) COMMENT 'Relationships between external organizations.';

DROP TABLE IF EXISTS master_external_org_postcode;
CREATE TABLE IF NOT EXISTS master_external_org_postcode (
  external_org INTEGER(15) NOT NULL COMMENT 'Reference to the organization in this postal code',
  postcode INTEGER(15) NOT NULL COMMENT 'Reference to the postal code of the location for this organization',
  valid_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Time of creation of this record',
  valid_end TIMESTAMP NULL COMMENT 'Time after which this record is not considered valid',
  source INTEGER(15) NOT NULL COMMENT 'Reference to the source or provenance for this record',
  source_comment VARCHAR(255) NULL COMMENT 'Additional notes on the provenance of this record',
  PRIMARY KEY (external_org, postcode),
  FOREIGN KEY fk_master_external_org_postcode_org (external_org) REFERENCES master_external_org (id),
  FOREIGN KEY fk_master_external_org_postcode_postcode (postcode) REFERENCES postcode (id),
  FOREIGN KEY fk_master_external_org_postcode_source (source) REFERENCES master_data_source (id)
) COMMENT = 'Associates external locations with physical locations or mailing address, using postal code or approximations thereof.';
