DROP TABLE IF EXISTS thewur_ranking;
CREATE TABLE IF NOT EXISTS thewur_ranking (
  id INTEGER(15) NOT NULL AUTO_INCREMENT,
  college INTEGER(15) NOT NULL COMMENT 'Reference to college or university being ranked',
  year YEAR NOT NULL COMMENT 'Year of the THE ranking',
  rank INTEGER(15) NOT NULL COMMENT 'The international ranking of the school, or the shared rank in case of tie, or the best rank in a bin or bucket',
  score DECIMAL(6,1) NOT NULL COMMENT 'The normalized aggregate score assigned by THE to the school',
  teaching_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized teaching score assigned by THE to the school',
  intl_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized international outlook score assigned by THE to the school',
  industry_income_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized industry income score assigned by THE to the school',
  research_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized research score assigned by THE to the school',
  citation_score DECIMAL(6,1) NOT NULL COMMENT 'The normalized citation score assigned by THE to the school',
  PRIMARY KEY (id),
  UNIQUE (college, year),
  FOREIGN KEY fk_arwu_college (college) REFERENCES college_university (id)
) COMMENT = 'Times Higher Education university ranking data.';
