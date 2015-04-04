SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate", 
         CASE
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||frbgrnt_sponsor_id   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||frbgrnt_sponsor_id
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_sponsor_id               
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_sponsor_id  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null 
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_title             
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null                       
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_title 
         else '00.070 Federal - Other'  
         END as "UniqueAwardNumber",
         frbgrnt_code as "RecipientAccountNumber",
         utl_raw.cast_to_raw( c => dbms_obfuscation_toolkit.md5( input_string => spriden_pidm ) ) as "Emp ID Number",
         CASE
         when substr(nbrjobs_ecls_code, 1,1) = 'A'
              then 'Faculty'
         when nbrjobs_ecls_code = 'MM'   
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '12' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'   
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BB' 
           and ntrpcls_pgrp_code = 99
              then 'Faculty'                                      
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'A3' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3A' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support'                  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3C' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3E' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3Z' 
           and ntrpcls_pgrp_code = 18
              then 'Research Support'      
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4G' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4Z' 
           and ntrpcls_pgrp_code = 10
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'                          
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code in (10, 12, 16, 18, 19) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code in (11, 12, 14, 16, 18)  
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code in (16, 19, 20)  
              then 'Research Support'                                                                               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('5A', '5B', '5C')  
           and ntrpcls_pgrp_code = 18
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7A' 
           and ntrpcls_pgrp_code = 20
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7E' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('7F', '7Z') 
           and ntrpcls_pgrp_code = 19
              then 'Research Support'           
          when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'AC' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = 'SA' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (05, 13) 
              then 'Technician/Staff Scientist'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (13, 15) 
              then 'Technician/Staff Scientist'                
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3D', '4B') 
           and ntrpcls_pgrp_code = 13
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3G', '4E') 
           and ntrpcls_pgrp_code = 15
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('CA', 'CC') 
           and ntrpcls_pgrp_code in (13, 15)
              then 'Technician/Staff Scientist'               
         when nbrjobs_ecls_code = 'GA'   
              then 'Grad Student'   
         when nbrjobs_ecls_code in ('PA', 'PB')   
              then 'Research Analyst/Coordinator' 
         when nbrjobs_ecls_code = 'RA'   
              then 'Post Graduate Researcher'
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'                          
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = '90' 
           and ntrpcls_pgrp_code = 99
              then 'Undergraduate Student'                            
-- the following line of code is used to research items that default because they don't match any of the 
-- above criteria                
--         else spriden_pidm||' '||nbrjobs_ecls_code||' '||ntrpcls_ecls_code||' '||ntrpcls_pgrp_code
         else 'Research Support'          
         END as "Occupational Classification",
--
-- need to get the average FTE status for the three month reporting period
        CASE  
         when ptrcaln_pict_code = 'BW'
         then
         CASE
           when avg(phrjobs_fte) > 1
            then 1
            else
             avg(phrjobs_fte)  
         END                 
         when ptrcaln_pict_code = 'MN'
         then 
            CASE
             when
             avg(phrjobs_fte) > 1
             then 1
             else
             avg(phrjobs_fte)
            END
         else 1
         END as "FTE Status", 
-- need to average the Prop Earn Alloc Award by the number of pay periods  
         CASE            
         when ptrcaln_pict_code = 'BW'
         then avg(phrelbd_percent/100) * avg(phrjobs_per_pay_salary) * avg(phrjobs_fte)/avg(phrearn_amt) 
         when ptrcaln_pict_code = 'MN'
         then avg(phrelbd_percent/100) * avg(phrjobs_per_pay_salary) * avg(phrjobs_fte)/avg(phrearn_amt)          
         else 9999
         END  as "Prop Earn Alloc Award" 
-- start of tables pulling data from                                                                     
    FROM phrelbd,
         nbrjobs a,
         ptrcaln,
         phrhist,
         frbgrnt,
         frvcfda,
         ftvfund,
         spriden,
         pebempl,
         nbbposn,
         ntrpcls,
         ftvorgn a,
         ftvorgn b,
         phrjobs,
         phrearn
          --   
   WHERE     ptrcaln_start_date <= '&&end_date'
         AND ptrcaln_start_date >= '&&beg_date'
         AND ptrcaln_end_date   >= '&&beg_date'
         ---
         AND ptrcaln_year       = phrhist_year
         AND ptrcaln_pict_code  = phrhist_pict_code
         AND ptrcaln_payno      = phrhist_payno
         AND phrhist_disp       >= 60
         ----     
--  and phrhist_year = '14'
--  and phrhist_pict_code = 'BW'
--  and phrhist_payno = '18'       
         AND phrelbd_pidm       = phrhist_pidm
         AND phrelbd_year       = phrhist_year
         AND phrelbd_pict_code  = phrhist_pict_code
         AND phrelbd_payno      = phrhist_payno
         AND phrelbd_seq_no     = phrhist_seq_no
         ---
         AND phrelbd_seq_no = 0
         AND phrelbd_coas_code = '&&coas'
         and phrelbd_effective_date >= '&&beg_date'
         and phrelbd_effective_date <= '&&end_date'         
         AND phrelbd_coas_code = ftvfund_coas_code
         AND phrelbd_fund_code = ftvfund_fund_code
         AND  ftvfund_ftyp_code in ('4A', '4C', '4E', '4Y', '4G')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')
         ---
--    and phrelbd_pidm = 2021848     
         AND a.nbrjobs_pidm = phrelbd_pidm
         AND a.nbrjobs_posn = phrelbd_posn
         AND a.nbrjobs_suff = phrelbd_suff
         AND (a.nbrjobs_fte > 0
         AND a.nbrjobs_fte <= 1)
         AND a.nbrjobs_effective_date =
                (SELECT MAX (nbrjobs_effective_date)
                   FROM nbrjobs b
                  WHERE     b.nbrjobs_pidm = a.nbrjobs_pidm
                        AND b.nbrjobs_posn = a.nbrjobs_posn
                        AND b.nbrjobs_suff = a.nbrjobs_suff
                        AND b.nbrjobs_effective_date <= phrelbd_effective_date)
          --
         AND frbgrnt_code = ftvfund_grnt_code
         --
         AND frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
         and spriden_pidm = a.nbrjobs_pidm
         and spriden_change_ind is null
         and pebempl_pidm = a.nbrjobs_pidm
         and nbbposn_posn = a.nbrjobs_posn
         and ntrpcls_code = nbbposn_pcls_code
         and phrjobs_pidm = phrelbd_pidm
         and phrjobs_year = phrelbd_year
         and phrjobs_pict_code = phrelbd_pict_code
         and phrjobs_payno = phrelbd_payno
         and phrjobs_posn =  phrelbd_posn
         and phrjobs_suff = phrelbd_suff
         and phrjobs_effective_date = phrelbd_effective_date         
       and a.ftvorgn_coas_code = nbrjobs_coas_code_ts 
         and a.ftvorgn_orgn_code = nbrjobs_orgn_code_ts
         and a.ftvorgn_eff_date <= '&&beg_date'
         and a.ftvorgn_nchg_date > '&&end_date'
         and (a.ftvorgn_term_date is null
           or a.ftvorgn_term_date < '&&end_date')
         and b.ftvorgn_coas_code = pebempl_coas_code_home
         and b.ftvorgn_orgn_code = pebempl_orgn_code_home        
         and b.ftvorgn_eff_date <= '&&beg_date'
         and b.ftvorgn_nchg_date > '&&end_date'
         and (b.ftvorgn_term_date is null
           or b.ftvorgn_term_date < '&&end_date')
         and phrearn_year = phrjobs_year
                AND phrearn_pict_code = phrjobs_pict_code
       AND phrearn_payno = phrjobs_payno
       AND phrearn_pidm = phrjobs_pidm
       AND phrearn_seq_no = phrjobs_seq_no
       AND phrearn_posn = phrjobs_posn
       AND phrearn_suff = phrjobs_suff
       and phrearn_amt > 0
       AND phrearn_effective_date = phrjobs_effective_date
       AND phrelbd_year = phrearn_year
       AND phrelbd_pict_code = phrearn_pict_code
       AND phrelbd_payno = phrearn_payno
       AND phrelbd_pidm = phrearn_pidm
       AND phrelbd_seq_no = phrearn_seq_no
       AND phrelbd_posn = phrearn_posn
       AND phrelbd_suff = phrearn_suff
       AND phrelbd_effective_date = phrearn_effective_date
       AND phrelbd_earn_code = phrearn_earn_code  
 group by          
        CASE   
         when substr(frvcfda_cfda_code, 4,3) = '000' 
             then '00.070 Federal - Other'
         when substr(frvcfda_cfda_code, 1,2) = '99'
             then '00.070 Federal - Other'   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is not null
             then'93.847 '||frbgrnt_sponsor_id   
         when frvcfda_cfda_code = '93.848' and frbgrnt_sponsor_id is null
             then'93.847 '||frbgrnt_title                                 
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no 
             then frvcfda_cfda_code||' '||frbgrnt_sponsor_id
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no = frvcfda_internal_id_no
             then frvcfda_cfda_code||' '||frbgrnt_title  
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_sponsor_id               
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_sponsor_id  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null 
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_title             
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null                       
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_title 
         else '00.070 Federal - Other' 
        END,
         frbgrnt_code,
         spriden_pidm,
        CASE
         when substr(nbrjobs_ecls_code, 1,1) = 'A'
              then 'Faculty'
         when nbrjobs_ecls_code = 'MM'   
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '12' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'   
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BB' 
           and ntrpcls_pgrp_code = 99
              then 'Faculty'                                      
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'A3' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3A' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support'                  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3C' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3E' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3Z' 
           and ntrpcls_pgrp_code = 18
              then 'Research Support'      
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4G' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4Z' 
           and ntrpcls_pgrp_code = 10
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'                          
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code in (10, 12, 16, 18, 19) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code in (11, 12, 14, 16, 18)  
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code in (16, 19, 20)  
              then 'Research Support'                                                                               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('5A', '5B', '5C')  
           and ntrpcls_pgrp_code = 18
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7A' 
           and ntrpcls_pgrp_code = 20
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7E' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('7F', '7Z') 
           and ntrpcls_pgrp_code = 19
              then 'Research Support'           
          when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'AC' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = 'SA' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (05, 13) 
              then 'Technician/Staff Scientist'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (13, 15) 
              then 'Technician/Staff Scientist'                
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3D', '4B') 
           and ntrpcls_pgrp_code = 13
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3G', '4E') 
           and ntrpcls_pgrp_code = 15
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('CA', 'CC') 
           and ntrpcls_pgrp_code in (13, 15)
              then 'Technician/Staff Scientist'               
         when nbrjobs_ecls_code = 'GA'   
              then 'Grad Student'   
         when nbrjobs_ecls_code in ('PA', 'PB')   
              then 'Research Analyst/Coordinator' 
         when nbrjobs_ecls_code = 'RA'   
              then 'Post Graduate Researcher'
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'                          
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = '90' 
           and ntrpcls_pgrp_code = 99
              then 'Undergraduate Student'                            
-- the following line of code is used to research items that default because they don't match any of the 
-- above criteria                
--         else spriden_pidm||' '||nbrjobs_ecls_code||' '||ntrpcls_ecls_code||' '||ntrpcls_pgrp_code
         else 'Research Support'  
        END,
         ptrcaln_pict_code    
union
SELECT  to_char( to_date('&&beg_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodStartDate",
         to_char(to_date('&&end_date', 'DD-MON-YYYY'), 'YYYY-MM-DD')  "PeriodEndDate", 
         CASE
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_sponsor_id               
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_sponsor_id  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null 
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_title             
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null                       
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_title 
         else '00.070 Federal - Other'  
         END as "UniqueAwardNumber",
         frbgrnt_code as "RecipientAccountNumber",
         utl_raw.cast_to_raw( c => dbms_obfuscation_toolkit.md5( input_string => spriden_pidm ) ) as "Emp ID Number",
         CASE
         when substr(nbrjobs_ecls_code, 1,1) = 'A'
              then 'Faculty'
         when nbrjobs_ecls_code = 'MM'   
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '12' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'   
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BB' 
           and ntrpcls_pgrp_code = 99
              then 'Faculty'                                      
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'A3' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3A' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support'                  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3C' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3E' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3Z' 
           and ntrpcls_pgrp_code = 18
              then 'Research Support'      
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4G' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4Z' 
           and ntrpcls_pgrp_code = 10
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'                          
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code in (10, 12, 16, 18, 19) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code in (11, 12, 14, 16, 18)  
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code in (16, 19, 20)  
              then 'Research Support'                                                                               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('5A', '5B', '5C')  
           and ntrpcls_pgrp_code = 18
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7A' 
           and ntrpcls_pgrp_code = 20
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7E' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('7F', '7Z') 
           and ntrpcls_pgrp_code = 19
              then 'Research Support'           
          when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'AC' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = 'SA' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (05, 13) 
              then 'Technician/Staff Scientist'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (13, 15) 
              then 'Technician/Staff Scientist'                
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3D', '4B') 
           and ntrpcls_pgrp_code = 13
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3G', '4E') 
           and ntrpcls_pgrp_code = 15
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('CA', 'CC') 
           and ntrpcls_pgrp_code in (13, 15)
              then 'Technician/Staff Scientist'               
         when nbrjobs_ecls_code = 'GA'   
              then 'Grad Student'   
         when nbrjobs_ecls_code in ('PA', 'PB')   
              then 'Research Analyst/Coordinator' 
         when nbrjobs_ecls_code = 'RA'   
              then 'Post Graduate Researcher'
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'                          
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = '90' 
           and ntrpcls_pgrp_code = 99
              then 'Undergraduate Student'                            
-- the following line of code is used to research items that default because they don't match any of the 
-- above criteria                
--         else spriden_pidm||' '||nbrjobs_ecls_code||' '||ntrpcls_ecls_code||' '||ntrpcls_pgrp_code
         else 'Research Support'          
         END as "Occupational Classification",
--
-- need to get the average FTE status for the three month reporting period
        CASE  
         when ptrcaln_pict_code = 'BW'
         then
         CASE
           when avg(phrjobs_fte) > 1
            then 1
            else
             avg(phrjobs_fte)  
         END                 
         when ptrcaln_pict_code = 'MN'
         then 
            CASE
             when
             avg(phrjobs_fte) > 1
             then 1
             else
             avg(phrjobs_fte)
            END
         else 1
         END as "FTE Status", 
-- need to average the Prop Earn Alloc Award by the number of pay periods  
         CASE            
         when ptrcaln_pict_code = 'BW'
         then avg(phrelbd_percent/100) * avg(phrjobs_per_pay_salary) * avg(phrjobs_fte)/avg(phrearn_amt) 
         when ptrcaln_pict_code = 'MN'
         then avg(phrelbd_percent/100) * avg(phrjobs_per_pay_salary) * avg(phrjobs_fte)/avg(phrearn_amt)          
         else 9999
         END  as "Prop Earn Alloc Award" 
-- start of tables pulling data from                                                                     
    FROM phrelbd,
         nbrjobs a,
         ptrcaln,
         phrhist,
         frbgrnt,
         ftvfund,
         spriden,
         pebempl,
         nbbposn,
         ntrpcls,
         ftvorgn a,
         ftvorgn b,
         phrjobs,
         phrearn
          --   
   WHERE     ptrcaln_start_date <= '&&end_date'
         AND ptrcaln_start_date >= '&&beg_date'
         AND ptrcaln_end_date   >= '&&beg_date'
         ---
         AND ptrcaln_year       = phrhist_year
         AND ptrcaln_pict_code  = phrhist_pict_code
         AND ptrcaln_payno      = phrhist_payno
         AND phrhist_disp       >= 60
         ----     
--  and phrhist_year = '14'
--  and phrhist_pict_code = 'BW'
--  and phrhist_payno = '18'       
         AND phrelbd_pidm       = phrhist_pidm
         AND phrelbd_year       = phrhist_year
         AND phrelbd_pict_code  = phrhist_pict_code
         AND phrelbd_payno      = phrhist_payno
         AND phrelbd_seq_no     = phrhist_seq_no
         ---
         AND phrelbd_seq_no = 0
         AND phrelbd_coas_code = '&&coas'
         and phrelbd_effective_date >= '&&beg_date'
         and phrelbd_effective_date <= '&&end_date'         
         AND phrelbd_coas_code = ftvfund_coas_code
         AND phrelbd_fund_code = ftvfund_fund_code
         AND  ftvfund_ftyp_code in ('4A', '4C', '4E', '4Y', '4G')
    and ftvfund_data_entry_ind = 'Y'
    and ftvfund_nchg_date = TO_DATE ('12/31/2099', 'MM/DD/YYYY')
         ---
--    and phrelbd_pidm = 2021848     
         AND a.nbrjobs_pidm = phrelbd_pidm
         AND a.nbrjobs_posn = phrelbd_posn
         AND a.nbrjobs_suff = phrelbd_suff
         AND (a.nbrjobs_fte > 0
         AND a.nbrjobs_fte <= 1)
         AND a.nbrjobs_effective_date =
                (SELECT MAX (nbrjobs_effective_date)
                   FROM nbrjobs b
                  WHERE     b.nbrjobs_pidm = a.nbrjobs_pidm
                        AND b.nbrjobs_posn = a.nbrjobs_posn
                        AND b.nbrjobs_suff = a.nbrjobs_suff
                        AND b.nbrjobs_effective_date <= phrelbd_effective_date)
          --
         AND frbgrnt_code = ftvfund_grnt_code
         --
         and frbgrnt_cfda_internal_id_no is null
         and spriden_pidm = a.nbrjobs_pidm
         and spriden_change_ind is null
         and pebempl_pidm = a.nbrjobs_pidm
         and nbbposn_posn = a.nbrjobs_posn
         and ntrpcls_code = nbbposn_pcls_code
         and phrjobs_pidm = phrelbd_pidm
         and phrjobs_year = phrelbd_year
         and phrjobs_pict_code = phrelbd_pict_code
         and phrjobs_payno = phrelbd_payno
         and phrjobs_posn =  phrelbd_posn
         and phrjobs_suff = phrelbd_suff
         and phrjobs_effective_date = phrelbd_effective_date         
       and a.ftvorgn_coas_code = nbrjobs_coas_code_ts 
         and a.ftvorgn_orgn_code = nbrjobs_orgn_code_ts
         and a.ftvorgn_eff_date <= '&&beg_date'
         and a.ftvorgn_nchg_date > '&&end_date'
         and (a.ftvorgn_term_date is null
           or a.ftvorgn_term_date < '&&end_date')
         and b.ftvorgn_coas_code = pebempl_coas_code_home
         and b.ftvorgn_orgn_code = pebempl_orgn_code_home        
         and b.ftvorgn_eff_date <= '&&beg_date'
         and b.ftvorgn_nchg_date > '&&end_date'
         and (b.ftvorgn_term_date is null
           or b.ftvorgn_term_date < '&&end_date')
         and phrearn_year = phrjobs_year
                AND phrearn_pict_code = phrjobs_pict_code
       AND phrearn_payno = phrjobs_payno
       AND phrearn_pidm = phrjobs_pidm
       AND phrearn_seq_no = phrjobs_seq_no
       AND phrearn_posn = phrjobs_posn
       AND phrearn_suff = phrjobs_suff
       and phrearn_amt > 0
       AND phrearn_effective_date = phrjobs_effective_date
       AND phrelbd_year = phrearn_year
       AND phrelbd_pict_code = phrearn_pict_code
       AND phrelbd_payno = phrearn_payno
       AND phrelbd_pidm = phrearn_pidm
       AND phrelbd_seq_no = phrearn_seq_no
       AND phrelbd_posn = phrearn_posn
       AND phrelbd_suff = phrearn_suff
       AND phrelbd_effective_date = phrearn_effective_date
       AND phrelbd_earn_code = phrearn_earn_code  
 group by          
        CASE   
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_sponsor_id               
         when frbgrnt_sponsor_id is not null and frbgrnt_cfda_internal_id_no is null
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_sponsor_id  
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null 
            and ftvfund_ftyp_code in ('4A', '4Y', '4G') 
         then '00.070 '||frbgrnt_title             
         when frbgrnt_sponsor_id is null and frbgrnt_cfda_internal_id_no is null                       
            and ftvfund_ftyp_code in ('4C', '4E') 
         then '00.000 '||frbgrnt_title 
         else '00.070 Federal - Other' 
        END,
         frbgrnt_code,
         spriden_pidm,
        CASE
         when substr(nbrjobs_ecls_code, 1,1) = 'A'
              then 'Faculty'
         when nbrjobs_ecls_code = 'MM'   
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '12' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 05
              then 'Faculty'   
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BB' 
           and ntrpcls_pgrp_code = 99
              then 'Faculty'                                      
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'A3' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3A' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support'                  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3C' 
           and ntrpcls_pgrp_code = 12
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3E' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '3Z' 
           and ntrpcls_pgrp_code = 18
              then 'Research Support'      
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4G' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '4Z' 
           and ntrpcls_pgrp_code = 10
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code = 14
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'                          
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (09, 10, 11, 12, 14) 
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CA' 
           and ntrpcls_pgrp_code in (10, 12, 16, 18, 19) 
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CC' 
           and ntrpcls_pgrp_code in (11, 12, 14, 16, 18)  
              then 'Research Support'   
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code in (16, 19, 20)  
              then 'Research Support'                                                                               
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('5A', '5B', '5C')  
           and ntrpcls_pgrp_code = 18
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7A' 
           and ntrpcls_pgrp_code = 20
              then 'Research Support'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = '7E' 
           and ntrpcls_pgrp_code = 16
              then 'Research Support'
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('7F', '7Z') 
           and ntrpcls_pgrp_code = 19
              then 'Research Support'           
          when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'AC' 
           and ntrpcls_pgrp_code = 11
              then 'Research Support' 
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = 'SA' 
           and ntrpcls_pgrp_code = 99
              then 'Research Support'               
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code in (05, 13) 
              then 'Technician/Staff Scientist'  
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = 'BA' 
           and ntrpcls_pgrp_code in (13, 15) 
              then 'Technician/Staff Scientist'                
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3D', '4B') 
           and ntrpcls_pgrp_code = 13
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('3G', '4E') 
           and ntrpcls_pgrp_code = 15
              then 'Technician/Staff Scientist' 
         when substr(nbrjobs_ecls_code, 1,1) = 'C' 
           and ntrpcls_ecls_code in ('CA', 'CC') 
           and ntrpcls_pgrp_code in (13, 15)
              then 'Technician/Staff Scientist'               
         when nbrjobs_ecls_code = 'GA'   
              then 'Grad Student'   
         when nbrjobs_ecls_code in ('PA', 'PB')   
              then 'Research Analyst/Coordinator' 
         when nbrjobs_ecls_code = 'RA'   
              then 'Post Graduate Researcher'
         when substr(nbrjobs_ecls_code, 1,1) = 'B' and ntrpcls_ecls_code = '32' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'  
         when substr(nbrjobs_ecls_code, 1,1) = 'C' and ntrpcls_ecls_code = 'CG' 
           and ntrpcls_pgrp_code = 15 
              then 'Clinicians'                          
         when substr(nbrjobs_ecls_code, 1,2) = 'SA' and ntrpcls_ecls_code = '90' 
           and ntrpcls_pgrp_code = 99
              then 'Undergraduate Student'                            
-- the following line of code is used to research items that default because they don't match any of the 
-- above criteria                
--         else spriden_pidm||' '||nbrjobs_ecls_code||' '||ntrpcls_ecls_code||' '||ntrpcls_pgrp_code
         else 'Research Support'  
        END,
         ptrcaln_pict_code    
         ;

