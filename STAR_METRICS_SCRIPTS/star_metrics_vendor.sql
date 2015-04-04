--Vendor query
-- substition variables
--     beg_date - start of period extracting
--     end_date - end of period extracting
--     fsyr     - fiscal year
--     period1  - first period
--     period2  - second period
--     period3  - third period
--     coas     - chart
--     lowerlimit - minimum amount for extracting data
--vendor, sponsor id is not null and cfda internal id no not null
--type is 4A, 4C, 4E, 4G, 4Y
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
         a.frbgrnt_code RecipientAccountNumber,
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'            
         END as "VendorDunsNumber",        
         d.fgbtrnd_trans_amt VendorPaymentAmount 
from frbgrnt a, frvcfda b, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select m.fgbtrnd_doc_code doccd, sum(m.fgbtrnd_trans_amt)
        from frbgrnt j, frvcfda k, ftvfund l, fgbtrnd m, fabinvh n,  spraddr o
      where j.frbgrnt_coas_code = '&&coas'
    and j.frbgrnt_sponsor_id is not null
    and j.frbgrnt_cfda_internal_id_no is not null
    and j.frbgrnt_cfda_internal_id_no = k.frvcfda_internal_id_no
    and l.ftvfund_grnt_code = j.frbgrnt_code  
    and l.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')     
    and l.ftvfund_data_entry_ind = 'Y'
    and l.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and m.fgbtrnd_coas_code = l.ftvfund_coas_code
    and m.fgbtrnd_fund_code = l.ftvfund_fund_code 
    and substr(m.fgbtrnd_acct_code, 1,3) != '156'
    and m.fgbtrnd_proc_code in ('O030', 'O033')
    and m.fgbtrnd_ledger_ind = 'O' 
    and m.fgbtrnd_fsyr_code = '&&fsyr'
    and m.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
    and m.fgbtrnd_doc_code in
         (select p.fgbtrnh_doc_code from fgbtrnh p  
           where p.fgbtrnh_vendor_pidm is not null
             and p.fgbtrnh_vendor_pidm not in 
            (select q.ftvvent_pidm from ftvvent q
             where q.ftvvent_vtyp_code = 'VE')) 
     and n.fabinvh_code = m.fgbtrnd_doc_code
     and n.fabinvh_vend_pidm not in 
     (select r.ftvvent_pidm from ftvvent r
       where r.ftvvent_vtyp_code = 'VE')
     AND o.spraddr_pidm = n.fabinvh_vend_pidm
     AND o.spraddr_atyp_code = n.fabinvh_atyp_code
     AND o.spraddr_seqno = n.fabinvh_atyp_seq_num
     AND (o.spraddr_from_date is null
       OR o.spraddr_from_date < n.fabinvh_pmt_due_date)
     AND (o.spraddr_to_date is null
       OR o.spraddr_to_date > n.fabinvh_pmt_due_date)   
      group by m.fgbtrnd_doc_code
      having sum(m.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs
  where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is not null
    and a.frbgrnt_cfda_internal_id_no = b.frvcfda_internal_id_no
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')     
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) != '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
    and d.fgbtrnd_doc_code in
         (select g.fgbtrnh_doc_code from fgbtrnh g  
           where g.fgbtrnh_vendor_pidm is not null
             and g.fgbtrnh_vendor_pidm not in 
            (select h.ftvvent_pidm from ftvvent h
             where h.ftvvent_vtyp_code = 'VE')) 
     and e.fabinvh_code = d.fgbtrnd_doc_code
     and e.fabinvh_vend_pidm not in 
     (select i.ftvvent_pidm from ftvvent i
       where i.ftvvent_vtyp_code = 'VE')
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                            
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num||' '||d.fgbtrnd_seq_num, a.frbgrnt_code,  
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
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
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
--vendor, sponsor id is not null, cfda internal id is null
--type is 4A, 4Y, 4C, 4E, 4G
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate",  
         CASE 
           when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is not null
           then '00.070 '||nvl(a.frbgrnt_sponsor_id, 0)
            when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is null
           then '00.070 '||a.frbgrnt_title         
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is not null            
           then '00.000 '||nvl(a.frbgrnt_sponsor_id, 0)
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is null            
           then '00.000 '||a.frbgrnt_title
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is not null
           then '00.200 '||nvl(a.frbgrnt_sponsor_id, 0) 
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is null
           then '00.200 '||a.frbgrnt_title 
           else '99.999 '||nvl(a.frbgrnt_sponsor_id, 0)         
         END as  "UniqueAwardNumber,",          
         a.frbgrnt_code RecipientAccountNumber,
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'  
         END as "VendorDunsNumber",        
         d.fgbtrnd_trans_amt "VendorPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select m.fgbtrnd_doc_code doccd, sum(m.fgbtrnd_trans_amt)
        from frbgrnt j, ftvfund l, fgbtrnd m, fabinvh n,  spraddr o
      where j.frbgrnt_coas_code = '&&coas'
    and j.frbgrnt_sponsor_id is not null
    and j.frbgrnt_cfda_internal_id_no is null
    and l.ftvfund_grnt_code = j.frbgrnt_code 
    and l.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')        
    and l.ftvfund_data_entry_ind = 'Y'
    and l.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and m.fgbtrnd_coas_code = l.ftvfund_coas_code
    and m.fgbtrnd_fund_code = l.ftvfund_fund_code 
    and substr(m.fgbtrnd_acct_code, 1,3) != '156'
    and m.fgbtrnd_proc_code in ('O030', 'O033')
    and m.fgbtrnd_ledger_ind = 'O' 
    and m.fgbtrnd_fsyr_code = '&&fsyr'
    and m.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
    and m.fgbtrnd_doc_code in
         (select p.fgbtrnh_doc_code from fgbtrnh p  
           where p.fgbtrnh_vendor_pidm is not null
             and p.fgbtrnh_vendor_pidm not in 
            (select q.ftvvent_pidm from ftvvent q
             where q.ftvvent_vtyp_code = 'VE')) 
     and n.fabinvh_code = m.fgbtrnd_doc_code
     and n.fabinvh_vend_pidm not in 
     (select r.ftvvent_pidm from ftvvent r
       where r.ftvvent_vtyp_code = 'VE')
     AND o.spraddr_pidm = n.fabinvh_vend_pidm
     AND o.spraddr_atyp_code = n.fabinvh_atyp_code
     AND o.spraddr_seqno = n.fabinvh_atyp_seq_num
     AND (o.spraddr_from_date is null
       OR o.spraddr_from_date < n.fabinvh_pmt_due_date)
     AND (o.spraddr_to_date is null
       OR o.spraddr_to_date > n.fabinvh_pmt_due_date)         
      group by m.fgbtrnd_doc_code
      having sum(m.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs
  where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is not null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code 
    and c.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')        
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) != '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')
    and d.fgbtrnd_doc_code in
         (select g.fgbtrnh_doc_code from fgbtrnh g  
           where g.fgbtrnh_vendor_pidm is not null
             and g.fgbtrnh_vendor_pidm not in 
            (select h.ftvvent_pidm from ftvvent h
             where h.ftvvent_vtyp_code = 'VE')) 
     and e.fabinvh_code = d.fgbtrnd_doc_code
     and e.fabinvh_vend_pidm not in 
     (select i.ftvvent_pidm from ftvvent i
       where i.ftvvent_vtyp_code = 'VE')
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)    
     and d.fgbtrnd_doc_code = Docs.doccd                                      
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num||' '||d.fgbtrnd_seq_num, a.frbgrnt_code,  
        CASE 
           when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is not null
           then '00.070 '||nvl(a.frbgrnt_sponsor_id, 0)
            when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is null
           then '00.070 '||a.frbgrnt_title         
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is not null            
           then '00.000 '||nvl(a.frbgrnt_sponsor_id, 0)
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is null            
           then '00.000 '||a.frbgrnt_title
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is not null
           then '00.200 '||nvl(a.frbgrnt_sponsor_id, 0) 
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is null
           then '00.200 '||a.frbgrnt_title 
           else '99.999 '||nvl(a.frbgrnt_sponsor_id, 0) 
       END, 
       CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
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
--vendor, sponsor id is null, cfda internal id no is not null
--type is 4A, 4C, 4E, 4G, 4Y
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
         a.frbgrnt_code RecipientAccountNumber,
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'   
         END as "VendorDunsNumber",        
         d.fgbtrnd_trans_amt VendorPaymentAmount 
from frbgrnt a, frvcfda b, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select m.fgbtrnd_doc_code doccd, sum(m.fgbtrnd_trans_amt)
        from frbgrnt j, frvcfda k, ftvfund l, fgbtrnd m, fabinvh n,  spraddr o
      where j.frbgrnt_coas_code = '&&coas'
    and j.frbgrnt_sponsor_id is null
    and j.frbgrnt_cfda_internal_id_no is not null
    and j.frbgrnt_cfda_internal_id_no = k.frvcfda_internal_id_no
    and l.ftvfund_grnt_code = j.frbgrnt_code  
    and l.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')      
    and l.ftvfund_data_entry_ind = 'Y'
    and l.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and m.fgbtrnd_coas_code = l.ftvfund_coas_code
    and m.fgbtrnd_fund_code = l.ftvfund_fund_code 
    and substr(m.fgbtrnd_acct_code, 1,3) != '156'
    and m.fgbtrnd_proc_code in ('O030', 'O033')
    and m.fgbtrnd_ledger_ind = 'O' 
    and m.fgbtrnd_fsyr_code = '&&fsyr'
    and m.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
     and m.fgbtrnd_doc_code in
         (select p.fgbtrnh_doc_code from fgbtrnh p  
           where p.fgbtrnh_vendor_pidm is not null
             and p.fgbtrnh_vendor_pidm not in 
            (select q.ftvvent_pidm from ftvvent q
             where q.ftvvent_vtyp_code = 'VE'))     
     and n.fabinvh_code = m.fgbtrnd_doc_code
     and n.fabinvh_vend_pidm not in 
     (select r.ftvvent_pidm from ftvvent r
       where r.ftvvent_vtyp_code = 'VE')
     AND o.spraddr_pidm = n.fabinvh_vend_pidm
     AND o.spraddr_atyp_code = n.fabinvh_atyp_code
     AND o.spraddr_seqno = n.fabinvh_atyp_seq_num
     AND (o.spraddr_from_date is null
       OR o.spraddr_from_date < n.fabinvh_pmt_due_date)
     AND (o.spraddr_to_date is null
       OR o.spraddr_to_date > n.fabinvh_pmt_due_date)   
      group by m.fgbtrnd_doc_code
      having sum(m.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs              
  where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is not null
    and a.frbgrnt_cfda_internal_id_no = b.frvcfda_internal_id_no
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')      
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) != '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')
     and d.fgbtrnd_doc_code in
         (select g.fgbtrnh_doc_code from fgbtrnh g  
           where g.fgbtrnh_vendor_pidm is not null
             and g.fgbtrnh_vendor_pidm not in 
            (select h.ftvvent_pidm from ftvvent h
             where h.ftvvent_vtyp_code = 'VE'))     
     and e.fabinvh_code = d.fgbtrnd_doc_code
     and e.fabinvh_vend_pidm not in 
     (select i.ftvvent_pidm from ftvvent i
       where i.ftvvent_vtyp_code = 'VE')
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)   
     and d.fgbtrnd_doc_code = Docs.doccd                                  
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num||' '||d.fgbtrnd_seq_num, a.frbgrnt_code,  
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
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
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
--vendor, sponsor id is null, cfda internal id no is null
--type is 4A, 4C, 4E, 4G, 4Y
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate", 
          CASE 
           when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is not null
           then '00.070 '||nvl(a.frbgrnt_sponsor_id, 0)
            when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is null
           then '00.070 '||a.frbgrnt_title         
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is not null            
           then '00.000 '||nvl(a.frbgrnt_sponsor_id, 0)
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is null            
           then '00.000 '||a.frbgrnt_title
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is not null
           then '00.200 '||nvl(a.frbgrnt_sponsor_id, 0) 
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is null
           then '00.200 '||a.frbgrnt_title 
           else '99.999 '||nvl(a.frbgrnt_sponsor_id, 0)            
         END as  "UniqueAwardNumber,",               
         a.frbgrnt_code "RecipientAccountNumber",
         CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
           when f.spraddr_natn_code is null then 'Z'||f.spraddr_zip
           when f.spraddr_natn_code = 'US' then 'Z'||f.spraddr_zip 
           when  f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
              and length(f.spraddr_zip) > 9
               then 'F000000000'               
           when f.spraddr_natn_code is not null and f.spraddr_natn_code != 'US'
                then 'F'||f.spraddr_zip             
           else 'Z00000-0000'  
         END as "VendorDunsNumber",        
         d.fgbtrnd_trans_amt "VendorPaymentAmount" 
from frbgrnt a, ftvfund c, fgbtrnd d, fabinvh e,  spraddr f,
     (select m.fgbtrnd_doc_code doccd, sum(m.fgbtrnd_trans_amt)
        from frbgrnt j, ftvfund l, fgbtrnd m, fabinvh n,  spraddr o
      where j.frbgrnt_coas_code = '&&coas'
    and j.frbgrnt_sponsor_id is null
    and j.frbgrnt_cfda_internal_id_no is null
    and l.ftvfund_grnt_code = j.frbgrnt_code  
    and l.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')       
    and l.ftvfund_data_entry_ind = 'Y'
    and l.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and m.fgbtrnd_coas_code = l.ftvfund_coas_code
    and m.fgbtrnd_fund_code = l.ftvfund_fund_code 
    and substr(m.fgbtrnd_acct_code, 1,3) != '156'
    and m.fgbtrnd_proc_code in ('O030', 'O033')
    and m.fgbtrnd_ledger_ind = 'O' 
    and m.fgbtrnd_fsyr_code = '&&fsyr'
    and m.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3') 
     and m.fgbtrnd_doc_code in
         (select p.fgbtrnh_doc_code from fgbtrnh p  
           where p.fgbtrnh_vendor_pidm is not null
             and p.fgbtrnh_vendor_pidm not in 
            (select q.ftvvent_pidm from ftvvent q
             where q.ftvvent_vtyp_code = 'VE'))     
     and n.fabinvh_code = m.fgbtrnd_doc_code
     and n.fabinvh_vend_pidm not in 
     (select r.ftvvent_pidm from ftvvent r
       where r.ftvvent_vtyp_code = 'VE')
     AND o.spraddr_pidm = n.fabinvh_vend_pidm
     AND o.spraddr_atyp_code = n.fabinvh_atyp_code
     AND o.spraddr_seqno = n.fabinvh_atyp_seq_num
     AND (o.spraddr_from_date is null
       OR o.spraddr_from_date < n.fabinvh_pmt_due_date)
     AND (o.spraddr_to_date is null
       OR o.spraddr_to_date > n.fabinvh_pmt_due_date)              
      group by m.fgbtrnd_doc_code
      having sum(m.fgbtrnd_trans_amt) > &&lowerlimit   ) Docs          
  where a.frbgrnt_coas_code = '&&coas'
    and a.frbgrnt_sponsor_id is null
    and a.frbgrnt_cfda_internal_id_no is null
    and c.ftvfund_grnt_code = a.frbgrnt_code  
    and c.ftvfund_ftyp_code in ('4A', '4C', '4E', '4G', '4Y')        
    and c.ftvfund_data_entry_ind = 'Y'
    and c.ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')      
    and d.fgbtrnd_coas_code = c.ftvfund_coas_code
    and d.fgbtrnd_fund_code = c.ftvfund_fund_code 
    and substr(d.fgbtrnd_acct_code, 1,3) != '156'
    and d.fgbtrnd_proc_code in ('O030', 'O033')
    and d.fgbtrnd_ledger_ind = 'O' 
    and d.fgbtrnd_fsyr_code = '&&fsyr'
    and d.fgbtrnd_posting_period in ('&&period1', '&&period2', '&&period3')
     and d.fgbtrnd_doc_code in
         (select g.fgbtrnh_doc_code from fgbtrnh g  
           where g.fgbtrnh_vendor_pidm is not null
             and g.fgbtrnh_vendor_pidm not in 
            (select h.ftvvent_pidm from ftvvent h
             where h.ftvvent_vtyp_code = 'VE'))     
     and e.fabinvh_code = d.fgbtrnd_doc_code
     and e.fabinvh_vend_pidm not in 
     (select i.ftvvent_pidm from ftvvent i
       where i.ftvvent_vtyp_code = 'VE')
     AND f.spraddr_pidm = e.fabinvh_vend_pidm
     AND f.spraddr_atyp_code = e.fabinvh_atyp_code
     AND f.spraddr_seqno = e.fabinvh_atyp_seq_num
     AND (f.spraddr_from_date is null
       OR f.spraddr_from_date < e.fabinvh_pmt_due_date)
     AND (f.spraddr_to_date is null
       OR f.spraddr_to_date > e.fabinvh_pmt_due_date)      
     and d.fgbtrnd_doc_code = Docs.doccd                                   
group by d.fgbtrnd_doc_code||' '||d.fgbtrnd_item_num||' '||d.fgbtrnd_seq_num, a.frbgrnt_code,  
       CASE 
           when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is not null
           then '00.070 '||nvl(a.frbgrnt_sponsor_id, 0)
            when c.ftvfund_ftyp_code in ('4A', '4Y') 
            and a.frbgrnt_sponsor_id is null
           then '00.070 '||a.frbgrnt_title         
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is not null            
           then '00.000 '||nvl(a.frbgrnt_sponsor_id, 0)
           when c.ftvfund_ftyp_code in ('4C', '4E')
            and a.frbgrnt_sponsor_id is null            
           then '00.000 '||a.frbgrnt_title
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is not null
           then '00.200 '||nvl(a.frbgrnt_sponsor_id, 0) 
           when c.ftvfund_ftyp_code = '4G' 
            and a.frbgrnt_sponsor_id is null
           then '00.200 '||a.frbgrnt_title 
           else '99.999 '||nvl(a.frbgrnt_sponsor_id, 0)  
       END, 
       CASE 
           when f.spraddr_zip is null and f.spraddr_natn_code is null
            and f.spraddr_stat_code = 'BC'
                then 'F000000000'           
           when f.spraddr_zip is null and f.spraddr_natn_code is null
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code = 'US'
                then 'Z00000-0000'  
           when f.spraddr_zip is null and f.spraddr_natn_code is not null
            and f.spraddr_natn_code != 'US'
                then 'F000000000'  
           when f.spraddr_natn_code is null and f.spraddr_stat_code = 'BC'
                then 'F'||f.spraddr_zip                                                     
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
;
