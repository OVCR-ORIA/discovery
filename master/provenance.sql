DROP TABLE IF EXISTS master_data_source;
CREATE TABLE IF NOT EXISTS master_data_source (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  name VARCHAR(15) NOT NULL COMMENT 'Human-readable label for data source',
  comment VARCHAR(255) NULL COMMENT 'Additional description of data source',
  PRIMARY KEY (id),
  UNIQUE (name)
) COMMENT = 'Controlled list of data sources or provenance for mastered entities.';
