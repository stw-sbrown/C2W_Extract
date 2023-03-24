create or replace
PROCEDURE P_SAP_DEL_SCM_TE_MO(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: SAP Service Component
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_SCM_TE_MO.sql
--
-- Subversion $Revision: 5251 $
--
-- CREATED        : 10/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP SCM upload file
--                  Queries Transform tables and populates table SAP_DEL_SCMTEMO
--                  Writes to file SAP_DEL_SCMTEMO_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      10/06/2016  K.Burton   Initial Draft
-- V 0.02      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 0.03      12/07/2016  K.Burton   CR_009 - Added CALD_TXT (D4003) to INSTE section
--                                    CR_011 - Added D2003 (special agreement factor) and D2004
--                                    (special agreement flag)_ to INSTE section
-- V 0.04      14/07/2016  K.Burton   SI-029 - Duplicate key rows - cursor query updated
-- V 0.05      28/07/2016  K.Burton   Defect 132 / SI-030 - special agreement factor/flag for TE
--                                    needs to come from MO_DISCHARGE_POINT
-- V 0.06      24/08/2016  K.Burton   CR_022 - Added Yearly Volume Estimate to INSTE section
-- V 0.07      25/08/2016  K.Burton   Defect 194 - removed join to MO_SERVICE_COMPONENT in main cursor
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_SCM_TE_MO';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_keys_written                NUMBER;
  
  l_rec_written                 BOOLEAN;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  
  l_count                       NUMBER;
  l_delimiter                   VARCHAR2(1) := '|';
--  l_delimiter                   VARCHAR2(1) := chr(9); 
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100); 
  l_filehandle UTL_FILE.FILE_TYPE;
  l_sql VARCHAR2(2000);
  
  l_timestamp VARCHAR2(20);
  l_prefix VARCHAR2(6) := 'TMO_';

  CURSOR cur_tab IS
    SELECT /*+ use_hash(mdp,msc) full(cd) full(dv) full(msc)  */
      DISTINCT l_prefix || NVL(TE.SAPFLOCNUMBER,TE.STWPROPERTYNUMBER_PK) || '_' || NVL(CD.CALCDISCHARGEID_PK,MDP.DPID_PK) || '_' || MDP.SERVICECOMPTYPE LEGACYRECNUM, -- V 0.04 
      TE.LEGACYRECNUM PARENTLEGACYRECNUM,
      MDP.SCEFFECTIVEFROMDATE,
      MDP.NO_IWCS,
      MDP.DPID_PK,
--      MSC.SPECIALAGREEMENTFACTOR, -- V 0.03 (D2003)
--      MSC.SPECIALAGREEMENTFLAG, -- V 0.03 (D2004)
      MDP.DPIDSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR, -- D2003 V 0.05 
      MDP.DPIDSPECIALAGREEMENTINPLACE SPECIALAGREEMENTFLAG, -- D2004 V 0.05 
      CD.TEYEARLYVOLESTIMATE, -- D2010 -- V 0.06
      MDP.AMMONIANITROCAL,
      MDP.CHARGEABLEDAILYVOL,
      MDP.CHEMICALOXYGENDEMAND,
      MDP.SUSPENDEDSOLIDSLOAD,
      MDP.TREFODCHEMOXYGENDEMAND, -- D6006
      MDP.TREFODCHEMSUSPSOLDEMAND, --D6007
      DV.NOTIFIEDVOLUME,
      MDP.DOMMESTICALLOWANCE,
      MDP.SEASONALFACTOR,
      MDP.TREFODCHEMAMONIANITROGENDEMAND,
      MDP.PERCENTAGEALLOWANCE,
      MDP.FIXEDALLOWANCE,
      MDP.RECEPTIONTREATMENTINDICATOR,
      MDP.PRIMARYTREATMENTINDICATOR,
      MDP.MARINETREATMENTINDICATOR,
      MDP.BIOLOGICALTREATMENTINDICATOR,
      MDP.SLUDGETREATMENTINDICATOR,
      MDP.AMMONIATREATMENTINDICATOR,
      MDP.TARRIFCODE,
      CD.DISCHARGETYPE,
      CD.CALCDISCHARGEID_PK,
      CD.TETARIFFBAND TARRIFBAND,
      CD.SUBMISSIONFREQ,
      MDP.TREFODCHEMCOMPXDEMAND,
      MDP.TREFODCHEMCOMPYDEMAND,
      MDP.TREFODCHEMCOMPZDEMAND,
      MDP.TEFXTREATMENTINDICATOR,
      MDP.TEFYTREATMENTINDICATOR,
      MDP.TEFZTREATMENTINDICATOR,
      MDP.TEFAVAILABILITYDATAX,
      MDP.TEFAVAILABILITYDATAY,
      MDP.TEFAVAILABILITYDATAZ,
      MDP.SEWERAGEVOLUMEADJMENTHOD,
      NULL CALD_TXT, -- V 0.03 (D4003)
      MDP.SPID_PK,
      TE.SAPFLOCNUMBER,
      TE.STWPROPERTYNUMBER_PK
    FROM SAPTRAN.MO_DISCHARGE_POINT MDP,
      SAPTRAN.MO_CALCULATED_DISCHARGE CD,
      SAPTRAN.MO_DISCHARGED_VOLUME DV,
--      SAPTRAN.MO_SERVICE_COMPONENT MSC, -- V 0.07 
      SAP_DEL_SCMTE TE
    WHERE MDP.SPID_PK = TE.SPID_PK
    AND MDP.DPID_PK = TE.DPID_PK -- V 0.04    
--    AND MDP.SPID_PK = MSC.SPID_PK -- V 0.07 
    AND MDP.DPID_PK = CD.DPID_PK(+)
    AND MDP.DPID_PK = DV.DPID_PK(+);
  
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)
    SELECT 7 COL_COUNT, -- indicates that rows of type HEADER will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A HEADER' SECTION_ID,  -- work around to get sections is sequence
      NULL COL_01,
      NULL COL_02,
      PARENTLEGACYRECNUM COL_03,
      NULL COL_04
    FROM SAP_DEL_SCMTEMO
    ORDER BY LEGACYRECNUM;
  
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,
                                                             COL_11,COL_12,COL_13,COL_14,COL_15,COL_16,COL_17,COL_18,COL_19,COL_20,
                                                             COL_21,COL_22,COL_23,COL_24,COL_25,COL_26,COL_27,COL_28,COL_29,COL_30,
                                                             COL_31,COL_32,COL_33,COL_34,COL_35,COL_36,COL_37,COL_38)
    SELECT 40 COL_COUNT,  -- indicates that rows of type INSTE will have max 40 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B INSTE' SECTION_ID, -- work around to get sections is sequence
      TO_CHAR(SCEFFECTIVEFROMDATE,'YYYYMMDD') COL_01,
      SPECIALAGREEMENTFACTOR COL_02, -- V 0.03 (D2003)
      SPECIALAGREEMENTFLAG COL_03, -- V 0.03 (D2004)    
      TEYEARLYVOLESTIMATE COL_04, -- V 0.06 (D2010)
      AMMONIANITROCAL COL_05,
      CHARGEABLEDAILYVOL COL_06,
      CHEMICALOXYGENDEMAND COL_07,
      SUSPENDEDSOLIDSLOAD COL_08,
      TREFODCHEMOXYGENDEMAND COL_09, -- D6006
      TREFODCHEMSUSPSOLDEMAND COL_10, -- D6007
      NOTIFIEDVOLUME COL_11,
      DOMMESTICALLOWANCE COL_12,
      SEASONALFACTOR COL_13,
      TREFODCHEMAMONIANITROGENDEMAND COL_14,
      PERCENTAGEALLOWANCE COL_15,
      FIXEDALLOWANCE COL_16,
      RECEPTIONTREATMENTINDICATOR COL_17,
      PRIMARYTREATMENTINDICATOR COL_18,
      MARINETREATMENTINDICATOR COL_19,
      BIOLOGICALTREATMENTINDICATOR COL_20,
      SLUDGETREATMENTINDICATOR COL_21,
      AMMONIATREATMENTINDICATOR COL_22,
      TARRIFCODE COL_23,
      DISCHARGETYPE COL_24,
      CALCDISCHARGEID_PK COL_25,
      TARRIFBAND COL_26,
      SUBMISSIONFREQ COL_27,
      TREFODCHEMCOMPXDEMAND COL_28,
      TREFODCHEMCOMPYDEMAND COL_29,
      TREFODCHEMCOMPZDEMAND COL_30,
      TEFXTREATMENTINDICATOR COL_31,
      TEFYTREATMENTINDICATOR COL_32,
      TEFZTREATMENTINDICATOR COL_33,
      TEFAVAILABILITYDATAX COL_34,
      TEFAVAILABILITYDATAY COL_35,
      TEFAVAILABILITYDATAZ COL_36,
      SEWERAGEVOLUMEADJMENTHOD COL_37,
      CALD_TXT COL_38
    FROM SAP_DEL_SCMTEMO
    ORDER BY LEGACYRECNUM;
  
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_SCMTEMO
    ORDER BY LEGACYRECNUM;
    
    SELECT COUNT(DISTINCT KEY_COL)
    INTO l_row_count
    FROM SAP_DEL_OUTPUT;
    
    RETURN l_row_count;
  END PopulateOutputTable;

BEGIN
   -- initial variables
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
--   l_no_row_dropped_cb := 0;
   l_no_row_written := 0;
   l_no_row_war := 10;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_job.IND_STATUS := 'RUN';

   -- get job no and start job
   P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name,
                         l_job.NO_INSTANCE,
                         l_job.ERR_TOLERANCE,
                         l_job.EXP_TOLERANCE,
                         l_job.WAR_TOLERANCE,
                         l_job.NO_COMMIT,
                         l_job.NO_STREAM,
                         l_job.NO_RANGE_MIN,
                         l_job.NO_RANGE_MAX,
                         l_job.IND_STATUS);

   COMMIT;

   l_progress := 'processing ';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  OPEN cur_tab;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_tab BULK COLLECT INTO t_tab LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_tab.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_err.TXT_KEY := t_tab(i).LEGACYRECNUM; 
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_SCMTEMO ';

          INSERT INTO SAP_DEL_SCMTEMO VALUES t_tab(i);
          
          l_no_row_insert := l_no_row_insert + 1;          
        EXCEPTION
        WHEN OTHERS THEN
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;

             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_exp := l_no_row_exp + 1;

             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)
             THEN
                 CLOSE cur_tab;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_tab
     
    IF t_tab.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_tab

  COMMIT;
  CLOSE cur_tab;  
  
  --  write the header table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3800, l_no_row_read,    'SCM TEMO (Service Components) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3810, l_no_row_dropped, 'SCM TEMO (Service Components) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3820, l_no_row_insert, 'SCM TEMO (Service Components) inserted to SAP_DEL_SCMTEMO');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_SCMTEMO;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3825, l_keys_written, 'SCM TEMO (Service Components) distinct legacy keys inserted to SAP_DEL_SCMTEMO');  

   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3830, l_keys_written, 'SCM TEMO (Service Components) distinct legacy keys inserted into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3835, l_no_row_insert, 'SCM TEMO (Service Components) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_SCMTEMO_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''TMO\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3840, l_no_row_written, 'SCM TEMO (Service Components) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP76', 3845, l_keys_written, 'SCM TEMO (Service Components) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_SCMTEMO',
                           p_batch_no => no_batch,
                           p_filename => l_filename);

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
END P_SAP_DEL_SCM_TE_MO;
/
exit;