DROP TABLE IF EXISTS qswur_ranking;
CREATE TABLE IF NOT EXISTS qswur_ranking (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  college INTEGER(15) NOT NULL COMMENT 'Reference to college or university being ranked',
  year YEAR NOT NULL COMMENT 'Year of the QS ranking',
  rank INTEGER(15) NOT NULL COMMENT 'The international ranking of the school, or the shared rank in case of tie, or the best rank in a bin or bucket',
  score DECIMAL(6,1) NULL COMMENT 'The normalized aggregate score assigned by ARWU to the school',
  PRIMARY KEY (id),
  UNIQUE (college, year),
  FOREIGN KEY fk_qswur_college (college) REFERENCES college_university (id)
) COMMENT = 'Quacquarelli Symonds university ranking data.';
