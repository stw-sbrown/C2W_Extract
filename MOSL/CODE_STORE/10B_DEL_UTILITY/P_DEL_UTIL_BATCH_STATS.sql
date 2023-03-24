create or replace
PROCEDURE P_DEL_UTIL_BATCH_STATS AS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Batch Run Statistics Data
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_DEL_BATCH_STATS.sql
--
-- Subversion $Revision: 6420 $
--
-- CREATED        : 15/09/2016
--
-- DESCRIPTION    : Utility proc to create CSV files of batch statistics data
--
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      15/09/2016  D.Cheung   Initial Draft
-- V 0.02      21/09/2016  D.Cheung   Fix issue with del files linked to tran cursors
--                                    Consolidate input and measures files into combined recon set
-- V 0.03      22/11/2016  D.Cheung   Add OTHER_STATS reports
-- V 0.04      23/11/2016  D.Cheung   Changed Other_stats queries to exclude MO tables
-- V 0.05      25/11/2016  D.Cheung   Updated query on SC part of other_stats to exclude no service records
-----------------------------------------------------------------------------------------    
  l_text VARCHAR2(2000);
  l_rows_written VARCHAR2(10);
  l_delimiter VARCHAR2(1) := ',';
  l_filehandle UTL_FILE.FILE_TYPE;
  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(200);
  l_timestamp VARCHAR2(20) := TO_CHAR(SYSDATE,'YYYYMMDD_HH24MI');
  l_no_batch    NUMBER;
  l_no_batch_t  NUMBER;
  l_include_header  BOOLEAN := FALSE;
  
  CURSOR cur_tran_runtime IS
    SELECT * FROM (
        SELECT 1 stats_type,
            bs.no_batch,
            NULL job_instance,
            TO_CHAR(CAST(bs.ts_start AS DATE),'dd-mon-yyyy hh24:mi:ss')   process_start,
            TO_CHAR(CAST(bs.ts_update AS DATE),'dd-mon-yyyy hh24:mi:ss')  process_end,
            to_char(to_date(TRUNC((CAST(bs.ts_update AS DATE) - CAST(bs.ts_start AS DATE)) * (24*60*60)),'sssss'),'hh24:mi:ss') Run_Time,
            bs.batch_status             process_status,
            NULL                        job_txt_arg,
            NULL                        job_err_tolerance,
            NULL                        job_exp_tolerance,
            NULL                        job_war_tolerance,
            NULL                        job_err_total,
            NULL                        job_exc_total,
            NULL                        job_war_total
        FROM MOUTRAN.mig_batchstatus bs
        WHERE bs.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus)   -- batch_status = 'END'
    UNION ALL
        SELECT 2 stats_type,
            bs.no_batch,
            js.no_instance,
            TO_CHAR(CAST(js.ts_start AS DATE),'dd-mon-yyyy hh24:mi:ss')   process_start,
            TO_CHAR(CAST(js.ts_update AS DATE),'dd-mon-yyyy hh24:mi:ss')  process_end,
            to_char(to_date(TRUNC((CAST(js.ts_update AS DATE) - CAST(js.ts_start AS DATE)) * (24*60*60)), 'sssss'), 'hh24:mi:ss') Run_Time,
            js.ind_status               process_status,
            js.txt_arg                  job_txt_arg,
            js.err_tolerance            job_err_tolerance,
            js.exp_tolerance            job_exp_tolerance,
            js.war_tolerance            job_war_tolerance,
            NVL(job_counts.err_count,0) job_err_total,
            NVL(job_counts.exc_count,0) job_exc_total,
            NVL(job_counts.war_count,0) job_war_total
        FROM MOUTRAN.mig_batchstatus bs
        JOIN MOUTRAN.mig_jobstatus js ON bs.no_batch = js.no_batch
        LEFT OUTER JOIN 
          (SELECT no_batch,
               no_instance,
               SUM(err_count) err_count,
               SUM(exc_count) exc_count,
               SUM(war_count) war_count
            FROM (SELECT no_batch,
                       no_instance,
                       CASE WHEN ind_log = 'E' THEN log_count ELSE 0 END AS err_count,
                       CASE WHEN ind_log = 'X' THEN log_count ELSE 0 END AS exc_count,
                       CASE WHEN ind_log = 'W' THEN log_count ELSE 0 END AS war_count
                  FROM (SELECT no_batch,
                               no_instance,
                               ind_log,
                               count(*) log_count
                        FROM MOUTRAN.mig_errorlog 
                        GROUP BY no_batch, no_instance, ind_log
                        )
                  )
            GROUP BY no_batch,no_instance
            ) job_counts
            ON (js.no_batch = job_counts.no_batch AND js.no_instance = job_counts.no_instance)
        WHERE bs.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus)   -- batch_status = 'END'
    )
    ORDER BY no_batch DESC, stats_type, process_start;
    
    CURSOR cur_tran_err IS
        SELECT elog.no_batch,
          elog.no_instance,
          jstatus.txt_arg,
          TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
          elog.no_seq,
          elog.ind_log,
          elog.no_err,
          REPLACE(elog.txt_key,',',';') txt_key,
          REPLACE(elog.txt_data,',',';') txt_data,
          REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUTRAN.mig_errorlog elog
        JOIN MOUTRAN.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUTRAN.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus) AND elog.ind_log = 'E'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;
        
    CURSOR cur_tran_exc IS    
        SELECT elog.no_batch,
            elog.no_instance,
            jstatus.txt_arg,
            TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
            elog.no_seq,
            elog.ind_log,
            elog.no_err,
            REPLACE(elog.txt_key,',',';') txt_key,
            REPLACE(elog.txt_data,',',';') txt_data,
            REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUTRAN.mig_errorlog elog
        JOIN MOUTRAN.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUTRAN.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus) AND elog.ind_log = 'X'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;

    CURSOR cur_tran_war IS    
        SELECT elog.no_batch,
            elog.no_instance,
            jstatus.txt_arg,
            TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
            elog.no_seq,
            elog.ind_log,
            elog.no_err,
            REPLACE(elog.txt_key,',',';') txt_key,
            REPLACE(elog.txt_data,',',';') txt_data,
            REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUTRAN.mig_errorlog elog
        JOIN MOUTRAN.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUTRAN.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus) AND elog.ind_log = 'W'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;

    CURSOR cur_tran_sum IS
        SELECT * FROM (
            SELECT elog.no_batch,
                elog.no_instance,
                jstatus.txt_arg,
                elog.ind_log,
                elog.no_err,
                REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err,
                COUNT(*) Occurrences_no
            FROM MOUTRAN.mig_errorlog elog
            JOIN MOUTRAN.mig_errref   eref ON elog.no_err = eref.no_err
            LEFT OUTER JOIN MOUTRAN.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
            WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus)
            GROUP BY elog.no_batch, elog.no_instance, jstatus.txt_arg, elog.ind_log, elog.no_err, REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' ')
        UNION ALL
            SELECT elog.no_batch,
                NULL no_instance,
                NULL txt_arg,
                elog.ind_log,
                NULL no_err,
                DECODE(elog.ind_log, 'E','Total Number Of Errors', 'X','Total Number Of Exceptions', 'Total Number Of Warnings') txt_err,
                COUNT(*) Occurrences_no
            FROM MOUTRAN.mig_errorlog elog
            JOIN MOUTRAN.mig_errref   eref ON elog.no_err = eref.no_err
            LEFT OUTER JOIN MOUTRAN.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
            WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus)
            GROUP BY elog.no_batch, elog.ind_log
        )
        ORDER BY no_batch DESC, no_instance NULLS LAST, txt_arg, ind_log, no_err, txt_err;

    CURSOR cur_del_runtime IS
    SELECT * FROM (
        SELECT 1 stats_type,
            bs.no_batch,
            NULL job_instance,
            TO_CHAR(CAST(bs.ts_start AS DATE),'dd-mon-yyyy hh24:mi:ss')   process_start,
            TO_CHAR(CAST(bs.ts_update AS DATE),'dd-mon-yyyy hh24:mi:ss')  process_end,
            to_char(to_date(TRUNC((CAST(bs.ts_update AS DATE) - CAST(bs.ts_start AS DATE)) * (24*60*60)),'sssss'),'hh24:mi:ss') Run_Time,
            bs.batch_status             process_status,
            NULL                        job_txt_arg,
            NULL                        job_err_tolerance,
            NULL                        job_exp_tolerance,
            NULL                        job_war_tolerance,
            NULL                        job_err_total,
            NULL                        job_exc_total,
            NULL                        job_war_total
        FROM MOUDEL.mig_batchstatus bs
        WHERE bs.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus)   -- batch_status = 'END'
    UNION ALL
        SELECT 2 stats_type,
            bs.no_batch,
            js.no_instance,
            TO_CHAR(CAST(js.ts_start AS DATE),'dd-mon-yyyy hh24:mi:ss')   process_start,
            TO_CHAR(CAST(js.ts_update AS DATE),'dd-mon-yyyy hh24:mi:ss')  process_end,
            to_char(to_date(TRUNC((CAST(js.ts_update AS DATE) - CAST(js.ts_start AS DATE)) * (24*60*60)), 'sssss'), 'hh24:mi:ss') Run_Time,
            js.ind_status               process_status,
            js.txt_arg                  job_txt_arg,
            js.err_tolerance            job_err_tolerance,
            js.exp_tolerance            job_exp_tolerance,
            js.war_tolerance            job_war_tolerance,
            NVL(job_counts.err_count,0) job_err_total,
            NVL(job_counts.exc_count,0) job_exc_total,
            NVL(job_counts.war_count,0) job_war_total
        FROM MOUDEL.mig_batchstatus bs
        JOIN MOUDEL.mig_jobstatus js ON bs.no_batch = js.no_batch
        LEFT OUTER JOIN 
          (SELECT no_batch,
               no_instance,
               SUM(err_count) err_count,
               SUM(exc_count) exc_count,
               SUM(war_count) war_count
            FROM (SELECT no_batch,
                       no_instance,
                       CASE WHEN ind_log = 'E' THEN log_count ELSE 0 END AS err_count,
                       CASE WHEN ind_log = 'X' THEN log_count ELSE 0 END AS exc_count,
                       CASE WHEN ind_log = 'W' THEN log_count ELSE 0 END AS war_count
                  FROM (SELECT no_batch,
                               no_instance,
                               ind_log,
                               count(*) log_count
                        FROM MOUDEL.mig_errorlog 
                        GROUP BY no_batch, no_instance, ind_log
                        )
                  )
            GROUP BY no_batch,no_instance
            ) job_counts
            ON (js.no_batch = job_counts.no_batch AND js.no_instance = job_counts.no_instance)
        WHERE bs.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus)   -- batch_status = 'END'
    )
    ORDER BY no_batch DESC, stats_type, process_start;
    
    CURSOR cur_del_err IS
        SELECT elog.no_batch,
          elog.no_instance,
          jstatus.txt_arg,
          TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
          elog.no_seq,
          elog.ind_log,
          elog.no_err,
          REPLACE(elog.txt_key,',',';') txt_key,
          REPLACE(elog.txt_data,',',';') txt_data,
          REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUDEL.mig_errorlog elog
        JOIN MOUDEL.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUDEL.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus) AND elog.ind_log = 'E'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;
        
    CURSOR cur_del_exc IS    
        SELECT elog.no_batch,
            elog.no_instance,
            jstatus.txt_arg,
            TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
            elog.no_seq,
            elog.ind_log,
            elog.no_err,
            REPLACE(elog.txt_key,',',';') txt_key,
            REPLACE(elog.txt_data,',',';') txt_data,
            REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUDEL.mig_errorlog elog
        JOIN MOUDEL.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUDEL.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus) AND elog.ind_log = 'X'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;

    CURSOR cur_del_war IS    
        SELECT elog.no_batch,
            elog.no_instance,
            jstatus.txt_arg,
            TO_CHAR(CAST(elog.ts_created as DATE),'dd-mon-yyyy hh24:mi:ss') ts_created,
            elog.no_seq,
            elog.ind_log,
            elog.no_err,
            REPLACE(elog.txt_key,',',';') txt_key,
            REPLACE(elog.txt_data,',',';') txt_data,
            REPLACE(REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' '),',',';') txt_err
        FROM MOUDEL.mig_errorlog elog
        JOIN MOUDEL.mig_errref   eref ON elog.no_err = eref.no_err
        LEFT OUTER JOIN MOUDEL.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
        WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus) AND elog.ind_log = 'W'
        ORDER BY elog.no_batch, elog.no_instance, elog.no_err, elog.no_seq;

    CURSOR cur_del_sum IS
        SELECT * FROM (
            SELECT elog.no_batch,
                elog.no_instance,
                jstatus.txt_arg,
                elog.ind_log,
                elog.no_err,
                REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' ') txt_err,
                COUNT(*) Occurrences_no
            FROM MOUDEL.mig_errorlog elog
            JOIN MOUDEL.mig_errref   eref ON elog.no_err = eref.no_err
            LEFT OUTER JOIN MOUDEL.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
            WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus)
            GROUP BY elog.no_batch, elog.no_instance, jstatus.txt_arg, elog.ind_log, elog.no_err, REPLACE(REPLACE(eref.txt_err,CHR(13),' '),CHR(10),' ')
        UNION ALL
            SELECT elog.no_batch,
                NULL no_instance,
                NULL txt_arg,
                elog.ind_log,
                NULL no_err,
                DECODE(elog.ind_log, 'E','Total Number Of Errors', 'X','Total Number Of Exceptions', 'Total Number Of Warnings') txt_err,
                COUNT(*) Occurrences_no
            FROM MOUDEL.mig_errorlog elog
            JOIN MOUDEL.mig_errref   eref ON elog.no_err = eref.no_err
            LEFT OUTER JOIN MOUDEL.mig_jobstatus jstatus ON elog.no_batch = jstatus.no_batch AND elog.no_instance = jstatus.no_instance
            WHERE elog.no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus)
            GROUP BY elog.no_batch, elog.ind_log
        )
        ORDER BY no_batch DESC, no_instance NULLS LAST, txt_arg, ind_log, no_err, txt_err;

    CURSOR cur_recon_input IS
        SELECT * FROM (
            SELECT 
                CASE WHEN LENGTH(cpref.recon_control_point) = 3 THEN SUBSTR(cpref.recon_control_point,1,2) || '0' || SUBSTR(cpref.recon_control_point,3,1) ELSE cpref.recon_control_point END AS recon_control_point,
                CASE WHEN LENGTH(cpref.recon_measure) < 4 THEN LPAD(cpref.recon_measure,4,'0') ELSE TO_CHAR(cpref.recon_measure) END AS recon_measure,
                cplog.recon_measure_total,
                cpref.recon_measure_desc
            FROM MOUTRAN.mig_cplog cplog
            JOIN MOUTRAN.mig_cpref cpref ON cplog.no_recon_cp = cpref.no_recon_cp
            WHERE no_batch = (SELECT MAX(no_batch) FROM MOUTRAN.mig_batchstatus)
        UNION ALL
            SELECT 
                CASE WHEN LENGTH(cpref.recon_control_point) = 3 THEN SUBSTR(cpref.recon_control_point,1,2) || '0' || SUBSTR(cpref.recon_control_point,3,1) ELSE cpref.recon_control_point END AS recon_control_point,
                CASE WHEN LENGTH(cpref.recon_measure) < 4 THEN LPAD(cpref.recon_measure,4,'0') ELSE TO_CHAR(cpref.recon_measure) END AS recon_measure,
                cplog.recon_measure_total,
                cpref.recon_measure_desc
            FROM MOUDEL.mig_cplog cplog
            JOIN MOUDEL.mig_cpref cpref ON cplog.no_recon_cp = cpref.no_recon_cp
            WHERE no_batch = (SELECT MAX(no_batch) FROM MOUDEL.mig_batchstatus)        
        UNION ALL
            -- Count of supply points by region
            SELECT 'CP79' recon_control_point,
                DECODE(wholesalerid_pk,
                    'ANGLIAN-W','3950',
                    'DWRCYMRU-W','3951',
                    'SEVERN-W','3952',
                    'SOUTHSTAFF-W','3953',
                    'THAMES-W','3954',
                    'UNITED-W','3955',
                    'WESSEX-W','3956',
                    'YORKSHIRE-W','3957',
                    '3960') recon_measure,
                row_count recon_measure_total,
                'Supply Points ' || wholesalerid_pk recon_measure_desc
            FROM (
                SELECT wholesalerid_pk, COUNT(*) row_count
                FROM MOUTRAN.mo_supply_point 
                GROUP BY wholesalerid_pk)
        )
        ORDER BY TO_NUMBER(SUBSTR(recon_control_point,3)), recon_measure;
        
    CURSOR cur_recon_measures IS
            SELECT 
                CASE WHEN LENGTH(recon_control_point) = 3 THEN SUBSTR(recon_control_point,1,2) || '0' || SUBSTR(recon_control_point,3,1)||'/'||TO_CHAR(recon_measure) ELSE recon_control_point||'/'||TO_CHAR(recon_measure) END AS cp_measure,
                recon_measure_desc
            FROM MOUTRAN.mig_cpref
        UNION ALL
            SELECT 
                CASE WHEN LENGTH(recon_control_point) = 3 THEN SUBSTR(recon_control_point,1,2) || '0' || SUBSTR(recon_control_point,3,1)||'/'||TO_CHAR(recon_measure) ELSE recon_control_point||'/'||TO_CHAR(recon_measure) END AS cp_measure,
                recon_measure_desc
            FROM MOUDEL.mig_cpref        
        ORDER BY cp_measure;
        
    CURSOR cur_other_stats_summary IS
        SELECT * FROM (
            --TOTAL NUMBER OF PROPERTIES BY WHOLESALER
            SELECT 10 AS STATNUM, 'TOTAL NUMBER OF PROPERTIES BY WHOLESALER' AS STATDESC
                , COUNT(DISTINCT ECT.NO_PROPERTY) AS TOTAL, NVL(OSP.OWC,'SEVERN-W') AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_SUPPLY_POINT MSP
                , CIS.ELIGIBILITY_CONTROL_TABLE ECT
                , RECEPTION.OWC_SUPPLY_POINT OSP
            WHERE ECT.CORESPID = SUBSTR(MSP.SPID_PK,1,10)
                AND MSP.SPID_PK = OSP.SPID_PK(+)
            GROUP BY OWC
        UNION
            --TOTAL NUMBER OF DISTINCT PROPERTIES WITH TE
            SELECT 20 AS STATNUM, 'TOTAL NUMBER OF DISTINCT PROPERTIES WITH TE' AS STATDESC
                , COUNT(DISTINCT ECT.NO_PROPERTY) AS TOTAL, NULL AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_DISCHARGE_POINT DDP
                , CIS.ELIGIBILITY_CONTROL_TABLE ECT
            WHERE ECT.CORESPID = SUBSTR(DDP.SPID_PK,1,10)
        UNION
            --TOTAL NUMBER OF CUSTOMERS
            SELECT 30 AS STATNUM, 'TOTAL NUMBER OF CUSTOMERS' AS STATDESC
                , COUNT(DISTINCT DSP.CUSTOMERNAME || '-' || DSP.CUSTADDRESSLINE01 || '-' || DSP.CUSTPOSTCODE ) AS TOTAL, NULL AS GROUP1, NULL AS GROUP2
            FROM DEL_SUPPLY_POINT DSP
        UNION
            --TOTAL SUPPLY_POINTS BY WHOLESALER
            SELECT 41 AS STATNUM, 'TOTAL SUPPLY_POINTS BY WHOLESALER' AS STATDESC
                , COUNT(DISTINCT DSP.SPID_PK) AS TOTAL, NVL(OSP.OWC,'SEVERN-W') AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_SUPPLY_POINT DSP
                , RECEPTION.OWC_SUPPLY_POINT OSP
            WHERE DSP.SPID_PK = OSP.SPID_PK(+)
            GROUP BY OWC
        UNION
            --TOTAL SUPPLY_POINTS BY PAIRINGREASONCODE
            SELECT 42 AS STATNUM, 'TOTAL SUPPLY_POINTS BY PAIRINGREASONCODE' AS STATDESC
                , COUNT(DISTINCT DSP.SPID_PK) AS TOTAL, DSP.PAIRINGREFREASONCODE AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_SUPPLY_POINT DSP
            GROUP BY DSP.PAIRINGREFREASONCODE  
        UNION
            --TOTAL SUPPLY_POINTS BY SERVICECATEGORY
            SELECT 43 AS STATNUM, 'TOTAL SUPPLY_POINTS BY SERVICECATEGORY' AS STATDESC
                , COUNT(DISTINCT DSP.SPID_PK) AS TOTAL, DSP.SERVICECATEGORY AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_SUPPLY_POINT DSP
            GROUP BY DSP.SERVICECATEGORY                
        UNION
            --TOTAL SERVICE COMPONENTS BY WHOLESALER
            SELECT 51 AS STATNUM, 'TOTAL SERVICE COMPONENTS BY WHOLESALER' AS STATDESC
                , COUNT(DISTINCT DSC.SPID_PK) AS TOTAL, NVL(OSP.OWC,'SEVERN-W') AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_SERVICE_COMPONENT DSC
                , RECEPTION.OWC_SUPPLY_POINT OSP
            WHERE DSC.SPID_PK = OSP.SPID_PK(+)
            AND NVL(METEREDPWTARIFFCODE,NVL(METEREDNPWTARIFFCODE,NVL(AWASSESSEDTARIFFCODE,NVL(UWUNMEASUREDTARIFFCODE,NVL(WADJCHARGEADJTARIFFCODE
                ,NVL(METEREDFSTARIFFCODE,NVL(ASASSESSEDTARIFFCODE,NVL(USUNMEASUREDTARIFFCODE,NVL(SADJCHARGEADJTARIFFCODE,NVL(SRFCWATERTARRIFCODE
                ,HWAYDRAINAGETARIFFCODE)))))))))) IS NOT NULL  
            GROUP BY OSP.OWC
        UNION
            --TOTAL SERVICE COMPONENTS BY TYPE
            SELECT 52 AS STATNUM, 'TOTAL SERVICE COMPONENTS BY TYPE' AS STATDESC
                , COUNT(DISTINCT SPID_PK) AS TOTAL, DSC.SERVICECOMPONENTTYPE AS GROUP1, NULL AS GROUP2
            FROM (
                SELECT SPID_PK, METEREDPWTARIFFCODE AS TARIFFCODE_PK, 'MPW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDPWTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, METEREDNPWTARIFFCODE AS TARIFFCODE_PK, 'MNPW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDNPWTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, AWASSESSEDTARIFFCODE AS TARIFFCODE_PK, 'AW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE AWASSESSEDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, UWUNMEASUREDTARIFFCODE AS TARIFFCODE_PK, 'UW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE UWUNMEASUREDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, WADJCHARGEADJTARIFFCODE AS TARIFFCODE_PK, 'WCA' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE WADJCHARGEADJTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, METEREDFSTARIFFCODE AS TARIFFCODE_PK, 'MS' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDFSTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, ASASSESSEDTARIFFCODE AS TARIFFCODE_PK, 'AS' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE ASASSESSEDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, USUNMEASUREDTARIFFCODE AS TARIFFCODE_PK, 'US' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE USUNMEASUREDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, SADJCHARGEADJTARIFFCODE AS TARIFFCODE_PK, 'SCA' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE SADJCHARGEADJTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, SRFCWATERTARRIFCODE AS TARIFFCODE_PK, 'SW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE SRFCWATERTARRIFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, HWAYDRAINAGETARIFFCODE AS TARIFFCODE_PK, 'HD' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE HWAYDRAINAGETARIFFCODE IS NOT NULL
            ) DSC
            GROUP BY DSC.SERVICECOMPONENTTYPE
        UNION
            --TOTAL DPIDS BY SVAM
            SELECT 61 AS STATNUM, 'TOTAL DPIDS BY SVAM' AS STATDESC
                ,COUNT(DISTINCT DDP.DPID_PK) AS TOTAL, DDP.SEWERAGEVOLUMEADJMENTHOD AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_DISCHARGE_POINT DDP
            GROUP BY DDP.SEWERAGEVOLUMEADJMENTHOD
        UNION
            --TOTAL METERS BY TREATMENT
            SELECT 70 AS STATNUM, 'TOTAL METERS BY TREATMENT' AS STATDESC 
                ,COUNT(DISTINCT MANUFACTURER_PK || '-' || MANUFACTURERSERIALNUM_PK) AS TOTAL, METERTREATMENT AS GROUP1, NULL AS GROUP2
            FROM MOUDEL.DEL_METER
            GROUP BY METERTREATMENT
        )
        ORDER BY STATNUM, GROUP1, GROUP2;
    
    CURSOR cur_other_stats_detail IS
        SELECT * FROM (
            --TOTAL SUPPLY_POINTS BY WHOLESALER AND PAIRINGREASONCODE
            SELECT 40 AS STATNUM, 'TOTAL SUPPLY_POINTS BY WHOLESALER AND PAIRINGREASONCODE' AS STATDESC
                , COUNT(DISTINCT DSP.SPID_PK) AS TOTAL, NVL(OSP.OWC,'SEVERN-W') AS GROUP1, DSP.PAIRINGREFREASONCODE AS GROUP2, NULL AS GROUP3
            FROM MOUDEL.DEL_SUPPLY_POINT DSP
                , RECEPTION.OWC_SUPPLY_POINT OSP
            WHERE DSP.SPID_PK = OSP.SPID_PK(+)
            GROUP BY OSP.OWC, DSP.PAIRINGREFREASONCODE
        UNION
            --TOTAL SERVICE COMPONENTS BY WHOLESALER, TYPE AND TARIFF
            SELECT 50 AS STATNUM, 'TOTAL SERVICE COMPONENTS BY WHOLESALER, TYPE AND TARIFF' AS STATDESC
                , COUNT(DSC.SPID_PK) AS TOTAL, NVL(OSP.OWC,'SEVERN-W') AS GROUP1, DSC.SERVICECOMPONENTTYPE AS GROUP2, DSC.TARIFFCODE_PK AS GROUP3
            FROM (
                SELECT SPID_PK, METEREDPWTARIFFCODE AS TARIFFCODE_PK, 'MPW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDPWTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, METEREDNPWTARIFFCODE AS TARIFFCODE_PK, 'MNPW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDNPWTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, AWASSESSEDTARIFFCODE AS TARIFFCODE_PK, 'AW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE AWASSESSEDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, UWUNMEASUREDTARIFFCODE AS TARIFFCODE_PK, 'UW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE UWUNMEASUREDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, WADJCHARGEADJTARIFFCODE AS TARIFFCODE_PK, 'WCA' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE WADJCHARGEADJTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, METEREDFSTARIFFCODE AS TARIFFCODE_PK, 'MS' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE METEREDFSTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, ASASSESSEDTARIFFCODE AS TARIFFCODE_PK, 'AS' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE ASASSESSEDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, USUNMEASUREDTARIFFCODE AS TARIFFCODE_PK, 'US' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE USUNMEASUREDTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, SADJCHARGEADJTARIFFCODE AS TARIFFCODE_PK, 'SCA' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE SADJCHARGEADJTARIFFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, SRFCWATERTARRIFCODE AS TARIFFCODE_PK, 'SW' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE SRFCWATERTARRIFCODE IS NOT NULL
                UNION
                SELECT SPID_PK, HWAYDRAINAGETARIFFCODE AS TARIFFCODE_PK, 'HD' AS SERVICECOMPONENTTYPE FROM DEL_SERVICE_COMPONENT
                WHERE HWAYDRAINAGETARIFFCODE IS NOT NULL
            ) DSC
                , RECEPTION.OWC_SUPPLY_POINT OSP 
            WHERE DSC.SPID_PK = OSP.SPID_PK(+)
            GROUP BY OSP.OWC, DSC.SERVICECOMPONENTTYPE, DSC.TARIFFCODE_PK
        UNION
            --TOTAL DPIDS BY SVAM AND TARIFF
            SELECT 60 AS STATNUM, 'TOTAL DPIDS BY SVAM AND TARIFF' AS STATDESC
                , COUNT(DISTINCT DDP.DPID_PK) AS TOTAL, DDP.SEWERAGEVOLUMEADJMENTHOD AS GROUP1, DDP.TARRIFCODE AS GROUP2, NULL AS GROUP3
            FROM MOUDEL.DEL_DISCHARGE_POINT DDP
            GROUP BY DDP.SEWERAGEVOLUMEADJMENTHOD, DDP.TARRIFCODE  
        )
        ORDER BY STATNUM, GROUP1, GROUP2;
        
BEGIN
   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;
   
   --**** PROCESS TRANSFORM FILES ****
   --GET BATCH NUMBER
   SELECT MAX(no_batch)
   INTO l_no_batch 
   FROM MOUTRAN.mig_batchstatus;
  
  BEGIN 
      -- create transform runtime CSV file
      l_filename := 'BATCH_STATS_MOUTRAN_1_RUNTIME_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'Stats_Type,No_Batch,No_Instance,TS_Start,TS_Update,Run_Time,Ind_Status,Job_Txt_Arg,Job_Err_tolerance,Job_Exp_tolerance,Job_War_tolerance,Err_Total,Exc_Total,War_Total');
      END IF;
      FOR t IN cur_tran_runtime
      LOOP
        l_text := t.stats_type || ',' || t.no_batch || ',' || t.job_instance || ',' || t.process_start || ',' || t.process_end || ',' || t.Run_Time || ',' || t.process_status || ',' || t.job_txt_arg || ',' || t.job_err_tolerance || ',' || t.job_exp_tolerance || ',' || t.job_war_tolerance || ',' || t.job_err_total || ',' || t.job_exc_total || ',' || t.job_war_total;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create transform errors CSV file
      l_filename := 'BATCH_STATS_MOUTRAN_2_ERRORS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_tran_err
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create transform exceptions CSV file
      l_filename := 'BATCH_STATS_MOUTRAN_3_EXCEPTIONS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_tran_exc
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create transform warnings CSV file
      l_filename := 'BATCH_STATS_MOUTRAN_4_WARNINGS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_tran_war
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create transform summary CSV file
      l_filename := 'BATCH_STATS_MOUTRAN_5_SUMMARY_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,Ind_Log,No_Err,TXT_Err,Ind_Count');
      END IF;
      FOR t IN cur_tran_sum
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_err || ',' || t.Occurrences_no;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  
  --**** PROCESS DELIVERY FILES ****
   --GET BATCH NUMBER
   l_no_batch_t := l_no_batch;
   SELECT MAX(no_batch)
   INTO l_no_batch 
   FROM MOUDEL.mig_batchstatus;
  
  BEGIN 
      -- create delivery runtime CSV file
      l_filename := 'BATCH_STATS_MOUDEL_1_RUNTIME_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'Stats_Type,No_Batch,No_Instance,TS_Start,TS_Update,Run_Time,Ind_Status,Job_Txt_Arg,Job_Err_tolerance,Job_Exp_tolerance,Job_War_tolerance,Err_Total,Exc_Total,War_Total');
      END IF;
      FOR t IN cur_del_runtime
      LOOP
        l_text := t.stats_type || ',' || t.no_batch || ',' || t.job_instance || ',' || t.process_start || ',' || t.process_end || ',' || t.Run_Time || ',' || t.process_status || ',' || t.job_txt_arg || ',' || t.job_err_tolerance || ',' || t.job_exp_tolerance || ',' || t.job_war_tolerance || ',' || t.job_err_total || ',' || t.job_exc_total || ',' || t.job_war_total;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create delivery errors CSV file
      l_filename := 'BATCH_STATS_MOUDEL_2_ERRORS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_del_err
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create delivery exceptions CSV file
      l_filename := 'BATCH_STATS_MOUDEL_3_EXCEPTIONS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_del_exc
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create delivery warnings CSV file
      l_filename := 'BATCH_STATS_MOUDEL_4_WARNINGS_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,TS_Created,No_Seq,Ind_Log,No_Err,TXT_Key,TXT_Data,TXT_Err');
      END IF;
      FOR t IN cur_del_war
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ts_created || ',' || t.no_seq || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_key || ',' || t.txt_data || ',' || t.txt_err;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create delivery summary CSV file
      l_filename := 'BATCH_STATS_MOUDEL_5_SUMMARY_' || l_timestamp || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'No_Batch,No_Instance,TXT_Arg,Ind_Log,No_Err,TXT_Err,Ind_Count');
      END IF;
      FOR t IN cur_del_sum
      LOOP
        l_text := t.no_batch || ',' || t.no_instance || ',' || t.txt_arg || ',' || t.ind_log || ',' || t.no_err || ',' || t.txt_err || ',' || t.Occurrences_no;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  
  --**** PROCESS RECONCILIATION FILES ****
  BEGIN 
      -- create reconciliation input values CSV file
      l_filename := 'BATCH_STATS_RECON_6_INPUT_' || l_timestamp || '_' || l_no_batch_t || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'Recon_Control_point,Recon_Measure,Recon_Measure_Total,Recon_Measure_Desc');
      END IF;
      FOR t IN cur_recon_input
      LOOP
        l_text := t.recon_control_point || ',="' || t.recon_measure || '",' || t.recon_measure_total || ',' || t.recon_measure_desc;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create reconciliation measures CSV file
      l_filename := 'BATCH_STATS_RECON_7_MEASURES_' || l_timestamp || '_' || l_no_batch_t || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'Control_Point_Measure, Measure_Description');
      END IF;
      FOR t IN cur_recon_measures
      LOOP
        l_text := t.cp_measure || ',' || t.recon_measure_desc;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  
  --**** PROCESS OTHER_STATS_REPORT FILES ****
  BEGIN 
      -- create other_stats_summary CSV file
      l_filename := 'OTHER_STATS_1_SUMMARY_' || l_timestamp || '_' || l_no_batch_t || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'STATNUM, STATDESC, TOTAL, GROUP1, GROUP2');
      END IF;
      FOR t IN cur_other_stats_summary
      LOOP
        l_text := t.statnum || ',' || t.statdesc || ',' || t.total || ',' || t.group1 || ',' || t.group2;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  BEGIN 
      -- create other_stats_detail CSV file
      l_filename := 'OTHER_STATS_2_DETAILS_' || l_timestamp || '_' || l_no_batch_t || '_' || l_no_batch || '.csv';
      l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  

      IF l_include_header THEN
          UTL_FILE.PUT_LINE(l_filehandle, 'STATNUM, STATDESC, TOTAL, GROUP1, GROUP2, GROUP3');
      END IF;
      FOR t IN cur_other_stats_detail
      LOOP
        l_text := t.statnum || ',' || t.statdesc || ',' || t.total || ',' || t.group1 || ',' || t.group2 || ',' || t.group3;
        UTL_FILE.PUT_LINE(l_filehandle, l_text);    
      END LOOP;
  
      UTL_FILE.FCLOSE(l_filehandle);  
  END;
  
END P_DEL_UTIL_BATCH_STATS;
/
exit;