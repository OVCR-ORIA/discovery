-- Sub Award
-- substition variables
--     beg_date - start of period extracting
--     end_date - end of period extracting
--     fsyr     - fiscal year
--     period1  - first period
--     period2  - second period
--     period3  - third period
--     coas     - chart
--     lowerlimit - minimum amount for extracting data
--sponsor id not null and cfda internal id no is not null
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
         a.frbgrnt_code "RecipientAccountNumber", 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",                 
         d.fgbtrnd_trans_amt "SubAwardPaymentAmount"
from frbgrnt a, frvcfda b, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, frvcfda h, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l     
      where g.frbgrnt_coas_code = '&&coas'
        and g.frbgrnt_sponsor_id is not null
       and g.frbgrnt_cfda_internal_id_no is not null
        and g.frbgrnt_cfda_internal_id_no = h.frvcfda_internal_id_no
        and i.ftvfund_grnt_code = g.frbgrnt_code  
       and i.ftvfund_data_entry_ind = 'Y'
        and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
        and j.fgbtrnd_coas_code = i.ftvfund_coas_code
        and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
       and substr(j.fgbtrnd_acct_code, 1,3) = '156'
        and j.fgbtrnd_proc_code in ('O030', 'O033')
        and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')  
        and k.fabinvh_code = j.fgbtrnd_doc_code
        AND l.spraddr_pidm = k.fabinvh_vend_pidm
        AND l.spraddr_atyp_code = k.fabinvh_atyp_code
        AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
        AND (l.spraddr_from_date is null
         OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
       AND (l.spraddr_to_date is null
         OR l.spraddr_to_date > k.fabinvh_pmt_due_date)          
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs     
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is not null
    and a.frbgrnt_cfda_internal_id_no = b.frvcfda_internal_id_no
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                     
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,   
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
      END,
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt 
union all
--sponsor id not null and cfda internal id no is null
--need to evaluate fund type to set unique award number
--4C or 4E
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",      
         '00.000'||' '||nvl(a.frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",          
         d.fgbtrnd_trans_amt "SubAwardPaymentAmount"          
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
       and g.frbgrnt_sponsor_id is not null
        and g.frbgrnt_cfda_internal_id_no is null
        and i.ftvfund_grnt_code = g.frbgrnt_code  
        and i.ftvfund_data_entry_ind = 'Y'
        and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')      
        and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
        and j.fgbtrnd_coas_code = i.ftvfund_coas_code
        and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
        and substr(j.fgbtrnd_acct_code, 1,3) = '156'
        and j.fgbtrnd_proc_code in ('O030', 'O033')
        and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')  
        and k.fabinvh_code = j.fgbtrnd_doc_code
         AND l.spraddr_pidm = k.fabinvh_vend_pidm
        AND l.spraddr_atyp_code = k.fabinvh_atyp_code
        AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
        AND (l.spraddr_from_date is null
          OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
        AND (l.spraddr_to_date is null
           OR l.spraddr_to_date > k.fabinvh_pmt_due_date)           
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code in ('4C', '4E')
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)      
     and d.fgbtrnd_doc_code = Docs.doccd                                    
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  nvl(a.frbgrnt_sponsor_id, 0), 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt         
union all
--sponsor id not null and cfda internal id no is null
--need to evaluate fund type to set unique award number
--4A or 4Y
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",    
         '00.070'||' '||nvl(a.frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",       
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
       and g.frbgrnt_sponsor_id is not null
       and g.frbgrnt_cfda_internal_id_no is null
       and i.ftvfund_grnt_code = g.frbgrnt_code  
       and i.ftvfund_data_entry_ind = 'Y'
       and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')
       and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
       and j.fgbtrnd_coas_code = i.ftvfund_coas_code
       and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
       and substr(j.fgbtrnd_acct_code, 1,3) = '156'
       and j.fgbtrnd_proc_code in ('O030', 'O033')
       and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')  
       and k.fabinvh_code = j.fgbtrnd_doc_code
       AND l.spraddr_pidm = k.fabinvh_vend_pidm
       AND l.spraddr_atyp_code = k.fabinvh_atyp_code
       AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
       AND (l.spraddr_from_date is null
         OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
       AND (l.spraddr_to_date is null
         OR l.spraddr_to_date > k.fabinvh_pmt_due_date)         
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code in ('4A', '4Y')
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)     
     and d.fgbtrnd_doc_code = Docs.doccd                                      
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  nvl(a.frbgrnt_sponsor_id, 0), 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt
union all
--sponsor id not null and cfda internal id no is null
--need to evaluate fund type to set unique award number
--4G
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
         '00.200'||' '||nvl(a.frbgrnt_sponsor_id, 0) "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",         
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
where g.frbgrnt_coas_code = '&&coas'
    and g.frbgrnt_sponsor_id is not null
    and g.frbgrnt_cfda_internal_id_no is null
    and i.ftvfund_grnt_code = g.frbgrnt_code  
    and i.ftvfund_data_entry_ind = 'Y'
    and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')
    and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and j.fgbtrnd_coas_code = i.ftvfund_coas_code
    and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
    and substr(j.fgbtrnd_acct_code, 1,3) = '156'
    and j.fgbtrnd_proc_code in ('O030', 'O033')
    and j.fgbtrnd_ledger_ind = 'O' 
    and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')    
     and k.fabinvh_code = j.fgbtrnd_doc_code
     AND l.spraddr_pidm = k.fabinvh_vend_pidm
     AND l.spraddr_atyp_code = k.fabinvh_atyp_code
     AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
     AND (l.spraddr_from_date is null
       OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
     AND (l.spraddr_to_date is null
       OR l.spraddr_to_date > k.fabinvh_pmt_due_date)        
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs   
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code = '4G'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                       
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  nvl(a.frbgrnt_sponsor_id, 0), 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'  
         END, 
         d.fgbtrnd_trans_amt 
union all
--sponsor id is null but cfda internal id is not null
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
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",         
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, frvcfda b, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, frvcfda h, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
        and g.frbgrnt_sponsor_id is null
        and g.frbgrnt_cfda_internal_id_no is not null
        and g.frbgrnt_cfda_internal_id_no = h.frvcfda_internal_id_no
        and i.ftvfund_grnt_code = g.frbgrnt_code  
        and i.ftvfund_data_entry_ind = 'Y'
        and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
        and j.fgbtrnd_coas_code = i.ftvfund_coas_code
        and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
        and substr(j.fgbtrnd_acct_code, 1,3) = '156'
        and j.fgbtrnd_proc_code in ('O030', 'O033')
        and j.fgbtrnd_ledger_ind = 'O' 
        and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
        and k.fabinvh_code = j.fgbtrnd_doc_code
        AND l.spraddr_pidm = k.fabinvh_vend_pidm
        AND l.spraddr_atyp_code = k.fabinvh_atyp_code
        AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
        AND (l.spraddr_from_date is null
          OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
        AND (l.spraddr_to_date is null
          OR l.spraddr_to_date > k.fabinvh_pmt_due_date)       
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is not null
    and a.frbgrnt_cfda_internal_id_no = b.frvcfda_internal_id_no
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                       
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  
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
         END, 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt 
union all
--sponsor id is null and cfda internal id is null
--need to evaluate fund type to set unique award number
--4C or 4E
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",     
         '00.000'||' '||a.frbgrnt_title "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",       
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
        and g.frbgrnt_sponsor_id is null
       and g.frbgrnt_cfda_internal_id_no is null
       and i.ftvfund_grnt_code = g.frbgrnt_code  
       and i.ftvfund_data_entry_ind = 'Y'
       and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')
       and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
       and j.fgbtrnd_coas_code = i.ftvfund_coas_code
        and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
        and substr(j.fgbtrnd_acct_code, 1,3) = '156'
        and j.fgbtrnd_proc_code in ('O030', 'O033')
       and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')  
      and k.fabinvh_code =j.fgbtrnd_doc_code
      AND l.spraddr_pidm = k.fabinvh_vend_pidm
       AND l.spraddr_atyp_code = k.fabinvh_atyp_code
       AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
       AND (l.spraddr_from_date is null
         OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
      AND (l.spraddr_to_date is null
        OR l.spraddr_to_date > k.fabinvh_pmt_due_date)        
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code in ('4C', '4E')
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')     
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                      
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  a.frbgrnt_title, 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt 
union all
--sponsor id is null and cfda internal id is null
--need to evaluate fund type to set unique award number
--4A and 4Y
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",       
         '00.070'||' '||a.frbgrnt_title "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'  
         END as "SubAwardVendorDunsNumber",      
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
        and g.frbgrnt_sponsor_id is null
        and g.frbgrnt_cfda_internal_id_no is null
        and i.ftvfund_grnt_code = g.frbgrnt_code  
        and i.ftvfund_data_entry_ind = 'Y'
        and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')
        and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
        and j.fgbtrnd_coas_code = i.ftvfund_coas_code
         and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
       and substr(j.fgbtrnd_acct_code, 1,3) = '156'
       and j.fgbtrnd_proc_code in ('O030', 'O033')
       and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
        and k.fabinvh_code = j.fgbtrnd_doc_code
        AND l.spraddr_pidm = k.fabinvh_vend_pidm
        AND l.spraddr_atyp_code = k.fabinvh_atyp_code
         AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
        AND (l.spraddr_from_date is null
           OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
         AND (l.spraddr_to_date is null
          OR l.spraddr_to_date > k.fabinvh_pmt_due_date)        
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code in ('4A', '4Y')
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')    
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                      
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  a.frbgrnt_title, 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'  
         END, 
         d.fgbtrnd_trans_amt 
union all
--sponsor id is null and cfda internal id is null
--need to evaluate fund type to set unique award number
--4G
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",      
         '00.200'||' '||a.frbgrnt_title "UniqueAwardNumber",
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END as "SubAwardVendorDunsNumber",       
        d.fgbtrnd_trans_amt "SubAwardPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select j.fgbtrnd_doc_code doccd, sum(j.fgbtrnd_trans_amt)
       from frbgrnt g, ftvfund i, fgbtrnd j, fabinvh k,  spraddr l       
      where g.frbgrnt_coas_code = '&&coas'
        and g.frbgrnt_sponsor_id is null
        and g.frbgrnt_cfda_internal_id_no is null
        and i.ftvfund_grnt_code = g.frbgrnt_code  
        and i.ftvfund_data_entry_ind = 'Y'
        and i.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')
        and i.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
        and j.fgbtrnd_coas_code = i.ftvfund_coas_code
        and j.fgbtrnd_fund_code = i.ftvfund_fund_code 
        and substr(j.fgbtrnd_acct_code, 1,3) = '156'
        and j.fgbtrnd_proc_code in ('O030', 'O033')
        and j.fgbtrnd_ledger_ind = 'O' 
       and j.fgbtrnd_fsyr_code = '&&fsyr'
        and j.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')    
         and k.fabinvh_code = j.fgbtrnd_doc_code
        AND l.spraddr_pidm = k.fabinvh_vend_pidm
        AND l.spraddr_atyp_code = k.fabinvh_atyp_code
        AND l.spraddr_seqno = k.fabinvh_atyp_seq_num
        AND (l.spraddr_from_date is null
          OR l.spraddr_from_date < k.fabinvh_pmt_due_date)
        AND (l.spraddr_to_date is null
          OR l.spraddr_to_date > k.fabinvh_pmt_due_date)       
      group by j.fgbtrnd_doc_code
      having sum(j.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs  
where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_ftyp_code = '4G'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) = '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')   
     and e.fabinvh_code = d.fgbtrnd_doc_code
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)    
     and d.fgbtrnd_doc_code = Docs.doccd                                     
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num,
         a.frbgrnt_code,  a.frbgrnt_title, 
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'                                       
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000' 
         END, 
         d.fgbtrnd_trans_amt; 
 