CREATE OR REPLACE PROCEDURE P_MOU_TRAN_PROPERTY(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_PROPERTY.sql
--
-- Subversion $Revision: 5284 $
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Property MO Extract 
--                 Will read from key gen and target tables, apply any transformationn
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 1.05      26/08/2016  S.Badhan   I-320. Lookup SAP_FLOC if currently null.
-- V 1.04      06/07/2016  S.Badhan   Set rateable value from highest value.
-- V 1.03      29/06/1016  L.Smith    Performance changes
-- V 1.02      22/04/2016  S.Badhan   Update to conditional setting of VOA BA Reference and
--                                    reason code after MOSL document update.
-- V 1.01      16/03/2016  S.Badhan   Changed to use new table name LU_CONSTRUCTION
--                                    For testing purposes set SAPFLOC number if null
-- V 1.00      15/03/2016  S.Badhan   Amended to use new keygen - BT_TVP054
-- V 0.04      08/03/2016  S.Badhan   On any sql exception set error flag to 'E' in 
--                                    call to FN_ERRORLOG.
-- V 0.03      03/03/2016  S.Badhan   Make sure TXT_DATA does not execeed max length 
--                                    Was causing errors in call to FN_ERRORLOG.
-- V 0.02      02/03/2016  S.Badhan   Added commments, check on tolerance levels and
--                                    check on reconciliation counts
-- V 0.01      25/02/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_PROPERTY';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_ELIGIBLE_PREMISES%ROWTYPE; 
  l_site                        LU_CONSTRUCTION_SITE%ROWTYPE; 
  l_hlt                         LU_PUBHEALTHRESITE%ROWTYPE; 
  l_floc                        LU_SAP_FLOCA%ROWTYPE;   
  l_alg                         CIS.TVP771SPRBLALGITEM%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
                     
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
    IS 
    SELECT  t054.NO_PROPERTY,
            t054.CD_PROPERTY_USE,
            t054.NO_SERV_PROV,
            t054.DT_START,
            t054.DT_END,
            t054.VOA_REFERENCE,
            t054.SAP_FLOC,
            t054.UPRN,
            t054.CORESPID
     FROM   BT_TVP054 t054
     WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end 
     order by t054.NO_PROPERTY, t054.DT_START desc;

                            
TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
BEGIN
 
   -- initial variables 
   
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_prev_prp := 0;
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
      
  -- start processing all records for range supplied
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY;
      l_mo := NULL;
      
      IF l_prev_prp <> t_prop(i).NO_PROPERTY THEN
      
          -- keep count of distinct property
         l_no_row_read := l_no_row_read + 1;
      
         IF t_prop(i).DT_END IS NULL THEN
            l_mo.OCCUPENCYSTATUS := 'OCCUPIED';
         ELSE
            l_mo.OCCUPENCYSTATUS := 'VACANT';     
         END IF;
  
         IF t_prop(i).VOA_REFERENCE IS NULL THEN
            l_mo.VOABAREFRSNCODE := 'OT';    
         ELSE
            l_mo.VOABAREFRSNCODE := NULL;
         END IF;
   
         BEGIN 
            SELECT BUILDINGWATERSTATUS
            INTO   l_site.BUILDINGWATERSTATUS
            FROM   LU_CONSTRUCTION_SITE
            WHERE  STWPROPERTYNUMBER_PK = t_prop(i).NO_PROPERTY;	
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_site.BUILDINGWATERSTATUS := 0;
         END;

         BEGIN 
            SELECT NONPUBHEALTHRELSITE,
                   NONPUBHEALTHRELSITEDSC,
                   PUBHEALTHRELSITEARR
            INTO   l_hlt.NONPUBHEALTHRELSITE,
                   l_hlt.NONPUBHEALTHRELSITEDSC,
                   l_hlt.PUBHEALTHRELSITEARR      
            FROM   LU_PUBHEALTHRESITE
            WHERE  STWPROPERTYNUMBER_PK = t_prop(i).NO_PROPERTY;	
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_hlt.NONPUBHEALTHRELSITE := 0;
              l_hlt.NONPUBHEALTHRELSITEDSC := NULL;
              l_hlt.PUBHEALTHRELSITEARR := 0;
         END;

        IF t_prop(i).UPRN IS NULL THEN
           l_mo.UPRNREASONCODE := 'OT';
        END IF;

        BEGIN 
           SELECT MAX(NO_VALUE)
           INTO   l_alg.NO_VALUE   
           FROM   BT_TVP054              t054,
                  CIS.TVP771SPRBLALGITEM alg
           WHERE  t054.NO_PROPERTY       = t_prop(i).NO_PROPERTY
           AND    t054.CD_COMPANY_SYSTEM = 'STW1'
           AND    alg.CD_COMPANY_SYSTEM  = t054.CD_COMPANY_SYSTEM
           AND    alg.NO_COMBINE_054     = t054.NO_COMBINE_054
           AND    alg.CD_BILL_ALG_ITEM   = 'RV';
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
             l_alg.NO_VALUE := NULL;
        END;
        l_mo.RATEABLEVALUE := l_alg.NO_VALUE;
        
        -- Get SAPFLOCNUMBER if null
        
        IF t_prop(i).SAP_FLOC IS NULL THEN
           l_progress := 'SELECT LU_SAP_FLOCA ';
           BEGIN
              SELECT SAPFLOCNUMBER
              INTO   l_floc.SAPFLOCNUMBER
              FROM   LU_SAP_FLOCA
              WHERE  STWPROPERTYNUMBER_PK  = t_prop(i).NO_PROPERTY;
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
                l_floc.SAPFLOCNUMBER := NULL;
           END;
           t_prop(i).SAP_FLOC := l_floc.SAPFLOCNUMBER;
        END IF;
        
        l_rec_written := TRUE;
        BEGIN 
          INSERT INTO MO_ELIGIBLE_PREMISES
          (STWPROPERTYNUMBER_PK, CORESPID_PK, CUSTOMERID_PK, SAPFLOCNUMBER, RATEABLEVALUE, PROPERTYUSECODE, 
          OCCUPENCYSTATUS, VOABAREFERENCE, VOABAREFRSNCODE, BUILDINGWATERSTATUS, NONPUBHEALTHRELSITE, 
          NONPUBHEALTHRELSITEDSC, PUBHEALTHRELSITEARR, SECTION154, UPRN, UPRNREASONCODE)
          VALUES
          (t_prop(i).NO_PROPERTY, t_prop(i).CORESPID, NULL, t_prop(i).SAP_FLOC, l_mo.RATEABLEVALUE, t_prop(i).CD_PROPERTY_USE,
          l_mo.OCCUPENCYSTATUS, t_prop(i).VOA_REFERENCE, l_mo.VOABAREFRSNCODE, l_site.BUILDINGWATERSTATUS, l_hlt.NONPUBHEALTHRELSITE, 
          l_hlt.NONPUBHEALTHRELSITEDSC, l_hlt.PUBHEALTHRELSITEARR, 0, t_prop(i).UPRN, l_mo.UPRNREASONCODE);
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
                 CLOSE cur_prop; 
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN; 
              END IF;
        END;
      
        -- keep count of records written
        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        END IF;
         
        l_prev_prp := t_prop(i).NO_PROPERTY;
      
      END IF;
      
    END LOOP;
    
    IF t_prop.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  
  CLOSE cur_prop;  

  -- write counts 
  l_progress := 'Writing Counts';  
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP26', 910, l_no_row_read,    'Distinct Eligible Properties read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP26', 920, l_no_row_dropped, 'Eligible Properties dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP26', 930, l_no_row_insert,  'Eligible Properties written to MO_ ELIGIBLE _PREMISE during Transform');    
 
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
END P_MOU_TRAN_PROPERTY;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_PROPERTY FOR P_MOU_TRAN_PROPERTY;

exit;
