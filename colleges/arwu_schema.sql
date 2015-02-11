DROP TABLE IF EXISTS arwu_ranking;
CREATE TABLE IF NOT EXISTS arwu_ranking (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  college INTEGER(15) NOT NULL COMMENT 'Reference to college or university being ranked',
  year YEAR NOT NULL COMMENT 'Year of the ARWU ranking',
  rank INTEGER(15) NOT NULL COMMENT 'The international ranking of the school, or the shared rank in case of tie, or the best rank in a bin or bucket',
  national_rank INTEGER(15) NOT NULL COMMENT 'The ranking relative to other schools in the same nation',
  score DECIMAL(6,1) NULL COMMENT 'The normalized aggregate score assigned by ARWU to the school',
  alumni_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized alumni score assigned by ARWU to the school',
  award_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized award score assigned by ARWU to the school',
  hici_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized HiCi score assigned by ARWU to the school',
  n_s_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized N&S score assigned by ARWU to the school',
  pub_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized PUB score assigned by ARWU to the school',
  pcp_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized PCP score assigned by ARWU to the school',
  PRIMARY KEY (id),
  FOREIGN KEY fk_arwu_college (college) REFERENCES college_university (id)
) COMMENT = 'Academic Ranking of World Universities ranking data.';
