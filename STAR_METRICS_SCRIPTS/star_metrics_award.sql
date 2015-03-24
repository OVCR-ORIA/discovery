-- Award data
-- substition variables
--     beg_date - start of period extracting
--     end_date - end of period extracting
--     fsyr     - fiscal year
--     period1  - first period
--     period2  - second period
--     period3  - third period
--     coas     - chart
-- revised 2/13/2015 mnevill to add fund type 4E to the selection with fund type 4C
-- 4A and 4Y
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",        
         CASE                  
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END as "UniqueAwardNumber",        
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
    and ftvfund_grnt_code = frbgrnt_code  
    and (ftvfund_ftyp_code = '4A' 
        or ftvfund_ftyp_code = '4Y')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')                    
group by frbgrnt_code,           
      CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
      END   
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate", 
--         frvcfda_cfda_code||' '||frbgrnt_title "UniqueAwardNumber",
          CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END as "UniqueAwardNumber",               
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no  
    and ftvfund_grnt_code = frbgrnt_code  
    and (ftvfund_ftyp_code = '4A' 
        or ftvfund_ftyp_code = '4Y')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
group by frbgrnt_code,  
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",               
         '00.070 '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",           
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no is null
    and ftvfund_grnt_code = frbgrnt_code  
    and (ftvfund_ftyp_code = '4A' 
        or ftvfund_ftyp_code = '4Y')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')                    
group by frbgrnt_code, nvl(frbgrnt_sponsor_id, 0) 
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate", 
         '00.070 '||frbgrnt_title "UniqueAwardNumber",            
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no is null   
    and ftvfund_grnt_code = frbgrnt_code  
    and (ftvfund_ftyp_code = '4A' 
        or ftvfund_ftyp_code = '4Y')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
group by frbgrnt_code,  frbgrnt_title         
union all
-- if 4C or 4E , no cfda code, use 00.200
-- for the rest of the uniqueawardnumber, if the frbgrnt_sponsor_id is not null, use it, otherwise use the frbgrnt_title
-- not 99.MULTI
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
--         frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END as "UniqueAwardNumber",          
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no  
    and frvcfda_cfda_code != '99.MULTI'  
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E') 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
group by frbgrnt_code,  
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other'
         END 
union all
-- not 99.MULTI
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
--         frvcfda_cfda_code||' '||frbgrnt_title "UniqueAwardNumber",
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END as "UniqueAwardNumber",           
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no   
    and frvcfda_cfda_code != '99.MULTI'        
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E') 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')    
group by frbgrnt_code,  
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other'
         END
union all
-- Not 99.MULTI
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
         '00.070 '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no  
    and frvcfda_cfda_code != '99.MULTI'  
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E')  
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')       
group by frbgrnt_code,  nvl(frbgrnt_sponsor_id, '0') 
union all
-- 99.MULTI
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
         '00.070 '||frbgrnt_title "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no   
    and frvcfda_cfda_code = '99.MULTI'        
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E')  
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')       
group by frbgrnt_code,  frbgrnt_title
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",        
         '00.000 '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no is null      
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E')  
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')      
group by frbgrnt_code,  nvl(frbgrnt_sponsor_id, '0') 
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
         '00.000 '||frbgrnt_title "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no is null      
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code in ('4C', '4E')  
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')      
group by frbgrnt_code,  frbgrnt_title
union all
-- if 4G , no cfda code, use 00.200
-- for the rest of the uniqueawardnumber, if the frbgrnt_sponsor_id is not null, use it, otherwise use the frbgrnt_title
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
--         frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other'
         END as "UniqueAwardNumber",         
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no   
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code = '4G' 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')    
group by frbgrnt_code,  
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other' 
         END 
union all
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",      
--         frvcfda_cfda_code||' '||frbgrnt_title "UniqueAwardNumber",
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other'
         END as "UniqueAwardNumber",         
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, frvcfda, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no    
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code = '4G' 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')      
group by frbgrnt_code,  
         CASE           
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||nvl(frbgrnt_sponsor_id, 0)   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||nvl(frbgrnt_sponsor_id, 0)
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
         then '00.070 '||nvl(frbgrnt_sponsor_id, 0)  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null  
         then '00.070 '||frbgrnt_title 
         else '00.070 Federal - Other'
         END
union all
--no cfda 
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",      
         '00.200 '||nvl(frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is not null
    and frbgrnt_cfda_internal_id_no is null      
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code = '4G' 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
group by frbgrnt_code,  nvl(frbgrnt_sponsor_id, '0') 
union all
--no cfda 
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",     
         '00.200 '||frbgrnt_title "UniqueAwardNumber",
         frbgrnt_code "RecipientAccountNumber",
         sum(fgbtrnd_trans_amt) "OverheadCharged"    
from frbgrnt, ftvfund, fgbtrnd
where frbgrnt_coas_code = '&&coas'
    and frbgrnt_sponsor_id is null
    and frbgrnt_cfda_internal_id_no is null      
    and ftvfund_grnt_code = frbgrnt_code  
    and ftvfund_ftyp_code = '4G' 
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and fgbtrnd_coas_code = ftvfund_coas_code
    and fgbtrnd_fund_code = ftvfund_fund_code 
    and  (substr(fgbtrnd_acct_code, 1,4) = '1981'
       and substr(fgbtrnd_acct_code, 1,5) != '19815'
       and substr(fgbtrnd_acct_code, 1,5) != '19814')
    and fgbtrnd_proc_code in ('O030', 'O033')
    and fgbtrnd_ledger_ind = 'O' 
    and fgbtrnd_fsyr_code = '&&fsyr'
    and fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
group by frbgrnt_code,  frbgrnt_title;