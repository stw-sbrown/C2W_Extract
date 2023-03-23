CREATE OR REPLACE
PROCEDURE P_MOU_TRAN_SERVICE_COMPONENT(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                       no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                                       return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SERVICE_COMPONENT.sql
--
-- Subversion $Revision: 4023 $
--
-- CREATED        : 17/03/2016
--
-- DESCRIPTION    : Procedure to create the Service Component MO Extract 
--                 Will read from the temporary tables containing preproccessed data
--                 for each service component type. Then collate by property each
--                 service component type details.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.08      18/05/2016  S.Badhan   Set special agreement default values as per latest
--                                    MOSL guidelines.
-- V 0.07      17/05/2016  S.Badhan   Use table MO_TARIFF instead of LU_TARIFF.
-- V 0.06      16/05/2016  S.Badhan   ID42. Amend effective start date to 01-APR-16.
-- V 0.05      27/04/2016  S.Badhan   I-190. Report where there are duplicates of Tarff and Component type
--                                    for SPID.
-- V 0.04      20/04/2016  S.Badhan   Updates to data validation from MOSL Initial Data Upload Req V1.1 
--                                    which is also the reason for System Test defect 9
-- V 0.03      18/04/2016  S.Badhan   Allow for SPID to be not found.
-- V 0.02      14/04/2016  S.Badhan   Remove columns METEREDFSTARIFFCODE , METEREDNPWTARIFFCODE ,
--                                    METEREDPWTARIFFCODE ,HWAYDRAINAGETARIFFCODE, ASSESSEDTARIFFCODE, 
--                                    SRFCWATERTARRIFCODE, UNMEASUREDTARIFFCODE.
-- V 0.01      17/03/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SERVICE_COMPONENT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_t314                        CIS.TVP314TARACCLSAPPL%ROWTYPE; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_SERVICE_COMPONENT%ROWTYPE; 
  l_sc                          MO_SERVICE_COMPONENT_TYPE%ROWTYPE; 
  l_age                         LU_TARIFF_SPECIAL_AGREEMENTS%ROWTYPE; 
  l_spid                        LU_SPID_RANGE%ROWTYPE;   
  l_mpw                         BT_SC_MPW%ROWTYPE; 
  l_spt                         BT_SP_TARIFF_SPLIT%ROWTYPE;    
  l_uw                          BT_SC_UW%ROWTYPE;   
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
            t054.NO_SERV_PROV,
            t054.CD_SERVICE_PROV,
            t054.NO_COMBINE_054,
            t054.CORESPID,
            tcat.SUPPLY_POINT_CODE,
            tcat.SERVICECOMPONENTTYPE,
            trf.CD_TARIFF, 
            trf.NO_TARIFF_GROUP, 
            trf.NO_TARIFF_SET, 
            trf.DT_START, 
            trf.DT_END
     FROM   BT_TVP054           t054,
            LU_SERVICE_CATEGORY tcat,
            BT_SPR_TARIFF       trf,
            MO_TARIFF           com
     WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end
     AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
     AND    com.TARIFFCODE_PK          = trf.CD_TARIFF
     AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV                          
     AND    tcat.SERVICECOMPONENTTYPE  = com.SERVICECOMPONENTTYPE   
     AND    tcat.SERVICECOMPONENTTYPE  <> 'TE'      
     ORDER BY t054.NO_PROPERTY, t054.NO_SERV_PROV, trf.NO_TARIFF_GROUP, trf.NO_TARIFF_SET;
                          
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
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY || ',' || t_prop(i).NO_SERV_PROV || ',' || t_prop(i).SERVICECOMPONENTTYPE || ',' || t_prop(i).CD_TARIFF;
      l_mo := NULL;
      
          -- keep count of distinct property
         l_no_row_read := l_no_row_read + 1;
         
         l_mo.SERVICECOMPONENTREF_PK := t_prop(i).NO_PROPERTY || t_prop(i).NO_SERV_PROV;
         l_mo.TARIFFCODE_PK	:= t_prop(i).CD_TARIFF;

         -- get supply point id

         l_progress := 'SELECT LU_SPID_RANGE ';   
         BEGIN 
           SELECT SPID_PK
           INTO   l_spid.SPID_PK
           FROM   LU_SPID_RANGE
           WHERE  CORESPID_PK     = t_prop(i).CORESPID
           AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;	
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_spid.SPID_PK := null;
         END;
            
         l_mo.SPID_PK := l_spid.SPID_PK;
         l_mo.DPID_PK := null;
         l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY;
         l_mo.STWSERVICETYPE	:= t_prop(i).CD_SERVICE_PROV;
         l_mo.SERVICECOMPONENTTYPE := t_prop(i).SERVICECOMPONENTTYPE;

         -- State of service component
         
         l_progress := 'SELECT TVP056SERVPROV ';   
         SELECT CASE  WHEN ST_SERV_PROV IN ('A', 'C', 'G')
                 THEN 1
                 ELSE 0
                END AS D2076_ACTIVE,
                DT_START
         INTO   l_mo.SERVICECOMPONENTENABLED,
                l_mo.EFFECTIVEFROMDATE
         FROM   CIS.TVP056SERVPROV
         WHERE  NO_PROPERTY  = t_prop(i).NO_PROPERTY  
         AND    NO_SERV_PROV = t_prop(i).NO_SERV_PROV;

         l_mo.EFFECTIVEFROMDATE := to_date('01/04/2016','dd/mm/yyyy');
         
         -- find any special agreements
        
         l_progress := 'SELECT LU_TARIFF_SPECIAL_AGREEMENTS ';   
         BEGIN 
           SELECT SPECIAL_AGREEMENT_FACTOR,
                  SPECIAL_AGREEMENT_FLAG,
                  OFWAT_REFERENCE_NUMBER
           INTO   l_age.SPECIAL_AGREEMENT_FACTOR,
                  l_age.SPECIAL_AGREEMENT_FLAG,
                  l_age.OFWAT_REFERENCE_NUMBER
           FROM   LU_TARIFF_SPECIAL_AGREEMENTS
           WHERE  PROPERTY_NO = t_prop(i).NO_PROPERTY
           AND    TARIFFCODE  = t_prop(i).CD_TARIFF
           AND    SERVICECOMPONENTTYPE = t_prop(i).SERVICECOMPONENTTYPE;	           
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_age.SPECIAL_AGREEMENT_FLAG := 'N';
         END;
              
         IF l_age.SPECIAL_AGREEMENT_FLAG = 'Y' THEN
            l_mo.SPECIALAGREEMENTFLAG := 1;
            l_mo.SPECIALAGREEMENTFACTOR := l_age.SPECIAL_AGREEMENT_FACTOR;
            l_mo.SPECIALAGREEMENTREF := l_age.OFWAT_REFERENCE_NUMBER;
         ELSE
            l_mo.SPECIALAGREEMENTFLAG := 0;
            l_mo.SPECIALAGREEMENTFACTOR := 0;
            l_mo.SPECIALAGREEMENTREF := 'NA';
         END IF;

     
      -- Metered Potable Water (MPW)

      l_mpw.D2079_MAXDAILYDMD := null;
      l_mpw.D2080_DLYRESVDCAP := NULL;
      l_mpw.D2056_TARIFFCODE := null;

      IF t_prop(i).SERVICECOMPONENTTYPE = 'MPW' THEN
         l_progress := 'SELECT BT_SC_MPW ';   
          BEGIN 
             SELECT D2079_MAXDAILYDMD,
                    D2080_DLYRESVDCAP,
                    D2056_TARIFFCODE
             INTO   l_mpw.D2079_MAXDAILYDMD,
                    l_mpw.D2080_DLYRESVDCAP,
                    l_mpw.D2056_TARIFFCODE
             FROM   BT_SC_MPW
             WHERE  NO_COMBINE_054 = t_prop(i).NO_COMBINE_054;	
          EXCEPTION
          WHEN NO_DATA_FOUND THEN
               null;
          END;
      END IF;  
      
      l_mo.METEREDPWMAXDAILYDEMAND := l_mpw.D2079_MAXDAILYDMD;
      l_mo.DAILYRESERVEDCAPACITY := l_mpw.D2080_DLYRESVDCAP;

      -- Metered Foul Sewage (MS)
     
      l_mo.METEREDFSMAXDAILYDEMAND := null;
      l_mo.METEREDFSDAILYRESVDCAPACITY := NULL;
      
      -- Metered Non Potable Water (MNPW)
                     
      l_mo.METEREDNPWMAXDAILYDEMAND	:= null;
      l_mo.METEREDNPWDAILYRESVDCAPACITY := null;

      -- Highway Drainage (HD)

      l_mo.HWAYSURFACEAREA	:= null;
      l_mo.HWAYCOMMUNITYCONFLAG := 0;
      
      -- Assessed (AW)      

      l_mo.ASSESSEDDVOLUMETRICRATE	:= null;
      l_mo.ASSESSEDCHARGEMETERSIZE := null;
      l_mo.ASSESSEDTARIFBAND := null;

      -- Surface Water (SW)      

      l_mo.SRFCWATERAREADRAINED	:= null;
      l_mo.SRFCWATERCOMMUNITYCONFLAG := 0;

      -- Unmeasured (UW)    
      
      l_uw.D2018_TYPEACOUNT := null;
      l_uw.D2019_TYPEBCOUNT := null;
      l_uw.D2020_TYPECCOUNT := null;                  
      l_uw.D2021_TYPEDCOUNT := null;                  
      l_uw.D2022_TYPEECOUNT := null;
      l_uw.D2024_TYPEFCOUNT := null;
      l_uw.D2046_TYPEGCOUNT := null;
      l_uw.D2048_TYPEHCOUNT := null;                
      l_uw.D2058_TYPEADESCRIPTION := null;
      l_uw.D2059_TYPEBDESCRIPTION := NULL;
      l_uw.D2060_TYPECDESCRIPTION := NULL;
      l_uw.D2061_TYPEDDESCRIPTION := null;
      l_uw.D2062_TYPEEDESCRIPTION := null;
      l_uw.D2064_TYPEFDESCRIPTION := null;
      l_uw.D2065_TYPEGDESCRIPTION := null;
      l_uw.D2069_TYPEHDESCRIPTION := NULL;
      l_uw.D2067_TARIFFCODE := NULL;
      
      IF t_prop(i).SERVICECOMPONENTTYPE = 'UW' THEN
         l_progress := 'SELECT BT_SC_UW ';         

         BEGIN 
             SELECT D2018_TYPEACOUNT,
                    D2019_TYPEBCOUNT,
                    D2020_TYPECCOUNT,                  
                    D2021_TYPEDCOUNT,                  
                    D2022_TYPEECOUNT,
                    D2024_TYPEFCOUNT,
                    D2046_TYPEGCOUNT,
                    D2048_TYPEHCOUNT,                  
                    D2058_TYPEADESCRIPTION,
                    D2059_TYPEBDESCRIPTION,
                    D2060_TYPECDESCRIPTION,
                    D2061_TYPEDDESCRIPTION,
                    D2062_TYPEEDESCRIPTION,
                    D2064_TYPEFDESCRIPTION,
                    D2065_TYPEGDESCRIPTION, 
                    D2069_TYPEHDESCRIPTION,   
                    D2067_TARIFFCODE
             INTO  l_uw.D2018_TYPEACOUNT,
                   l_uw.D2019_TYPEBCOUNT,
                   l_uw.D2020_TYPECCOUNT,                 
                   l_uw.D2021_TYPEDCOUNT,                 
                   l_uw.D2022_TYPEECOUNT,
                   l_uw.D2024_TYPEFCOUNT,
                   l_uw.D2046_TYPEGCOUNT,
                   l_uw.D2048_TYPEHCOUNT,       
                   l_uw.D2058_TYPEADESCRIPTION,
                   l_uw.D2059_TYPEBDESCRIPTION,
                   l_uw.D2060_TYPECDESCRIPTION,
                   l_uw.D2061_TYPEDDESCRIPTION,
                   l_uw.D2062_TYPEEDESCRIPTION,
                   l_uw.D2064_TYPEFDESCRIPTION,
                   l_uw.D2065_TYPEGDESCRIPTION,
                   l_uw.D2069_TYPEHDESCRIPTION,
                   l_uw.D2067_TARIFFCODE 
            FROM   BT_SC_UW
            WHERE  NO_COMBINE_054  = t_prop(i).NO_COMBINE_054
            AND    NO_TARIFF_GROUP = t_prop(i).NO_TARIFF_GROUP 
            AND    NO_TARIFF_SET   = t_prop(i).NO_TARIFF_SET; 
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              null;
         END;

      END IF;
      
      l_mo.UNMEASUREDTYPEACOUNT	:= l_uw.D2018_TYPEACOUNT;
      l_mo.UNMEASUREDTYPEBCOUNT := l_uw.D2019_TYPEBCOUNT;
      l_mo.UNMEASUREDTYPECCOUNT := l_uw.D2020_TYPECCOUNT;
      l_mo.UNMEASUREDTYPEDCOUNT	:= l_uw.D2021_TYPEDCOUNT;
      l_mo.UNMEASUREDTYPEECOUNT := l_uw.D2022_TYPEECOUNT;
      l_mo.UNMEASUREDTYPEFCOUNT := l_uw.D2024_TYPEFCOUNT;
      l_mo.UNMEASUREDTYPEGCOUNT := l_uw.D2046_TYPEGCOUNT;
      l_mo.UNMEASUREDTYPEHCOUNT := l_uw.D2048_TYPEHCOUNT;
      l_mo.UNMEASUREDTYPEADESCRIPTION := l_uw.D2058_TYPEADESCRIPTION;
      l_mo.UNMEASUREDTYPEBDESCRIPTION := l_uw.D2059_TYPEBDESCRIPTION;
      l_mo.UNMEASUREDTYPECDESCRIPTION := l_uw.D2060_TYPECDESCRIPTION;
      l_mo.UNMEASUREDTYPEDDESCRIPTION := l_uw.D2061_TYPEDDESCRIPTION;
      l_mo.UNMEASUREDTYPEEDESCRIPTION := l_uw.D2062_TYPEEDESCRIPTION;
      l_mo.UNMEASUREDTYPEFDESCRIPTION := l_uw.D2064_TYPEFDESCRIPTION;
      l_mo.UNMEASUREDTYPEGDESCRIPTION := l_uw.D2065_TYPEGDESCRIPTION;
      l_mo.UNMEASUREDTYPEHDESCRIPTION := l_uw.D2069_TYPEHDESCRIPTION;
      l_mo.PIPESIZE := NULL;
      
      -- Get translation tariff if tariffs have different charges based on the Zone or 
      -- PCODE Algorithm item

      l_progress := 'SELECT BT_SP_TARIFF_SPLIT ';  
      BEGIN      
        SELECT CD_SPLIT_TARIFF
        INTO   l_spt.CD_SPLIT_TARIFF
        FROM   BT_SP_TARIFF_SPLIT     spt,
               BT_SPR_TARIFF_ALGITEM  alg
        WHERE  alg.NO_COMBINE_054   = t_prop(i).NO_COMBINE_054
        AND    alg.CD_TARIFF        = spt.CD_TARIFF
        AND    alg.CD_BILL_ALG_ITEM = spt.CD_BILL_ALG_ITEM
        AND    alg.CD_REF_TAB       = spt.CD_REF_TAB;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_spt.CD_SPLIT_TARIFF := null;
      END;

      IF l_spt.CD_SPLIT_TARIFF IS NOT NULL THEN
         l_mo.TARIFFCODE_PK := l_spt.CD_SPLIT_TARIFF;
      END IF;

      l_progress := 'INSERT MO_SERVICE_COMPONENT ';           
      l_rec_written := TRUE;

        BEGIN 
          INSERT INTO MO_SERVICE_COMPONENT
          (SERVICECOMPONENTREF_PK, TARIFFCODE_PK, SPID_PK, DPID_PK, STWPROPERTYNUMBER_PK, STWSERVICETYPE, 
           SERVICECOMPONENTTYPE, SERVICECOMPONENTENABLED, EFFECTIVEFROMDATE,
           SPECIALAGREEMENTFACTOR, SPECIALAGREEMENTFLAG, SPECIALAGREEMENTREF, 
           METEREDPWMAXDAILYDEMAND, DAILYRESERVEDCAPACITY,
           METEREDNPWMAXDAILYDEMAND, METEREDNPWDAILYRESVDCAPACITY,
           METEREDFSMAXDAILYDEMAND, METEREDFSDAILYRESVDCAPACITY, 
           HWAYSURFACEAREA, HWAYCOMMUNITYCONFLAG, 
           ASSESSEDDVOLUMETRICRATE, ASSESSEDCHARGEMETERSIZE, ASSESSEDTARIFBAND, 
           SRFCWATERAREADRAINED, SRFCWATERCOMMUNITYCONFLAG, 
           UNMEASUREDTYPEACOUNT, UNMEASUREDTYPEBCOUNT, UNMEASUREDTYPECCOUNT, UNMEASUREDTYPEDCOUNT, 
           UNMEASUREDTYPEECOUNT, UNMEASUREDTYPEFCOUNT, UNMEASUREDTYPEGCOUNT, UNMEASUREDTYPEHCOUNT, 
           UNMEASUREDTYPEADESCRIPTION, UNMEASUREDTYPEBDESCRIPTION, UNMEASUREDTYPECDESCRIPTION, 
           UNMEASUREDTYPEDDESCRIPTION, UNMEASUREDTYPEEDESCRIPTION, UNMEASUREDTYPEFDESCRIPTION, 
           UNMEASUREDTYPEGDESCRIPTION, UNMEASUREDTYPEHDESCRIPTION)
           VALUES
           (l_mo.SERVICECOMPONENTREF_PK, l_mo.TARIFFCODE_PK, l_mo.SPID_PK, l_mo.DPID_PK, l_mo.STWPROPERTYNUMBER_PK, l_mo.STWSERVICETYPE,
           l_mo.SERVICECOMPONENTTYPE, l_mo.SERVICECOMPONENTENABLED, l_mo.EFFECTIVEFROMDATE,
           l_mo.SPECIALAGREEMENTFACTOR, l_mo.SPECIALAGREEMENTFLAG, l_mo.SPECIALAGREEMENTREF,
           l_mo.METEREDPWMAXDAILYDEMAND, l_mo.DAILYRESERVEDCAPACITY,
           l_mo.METEREDNPWMAXDAILYDEMAND, l_mo.METEREDNPWDAILYRESVDCAPACITY,
           l_mo.METEREDFSMAXDAILYDEMAND, l_mo.METEREDFSDAILYRESVDCAPACITY, 
           l_mo.HWAYSURFACEAREA, l_mo.HWAYCOMMUNITYCONFLAG, 
           l_mo.ASSESSEDDVOLUMETRICRATE, l_mo.ASSESSEDCHARGEMETERSIZE, l_mo.ASSESSEDTARIFBAND,
           l_mo.SRFCWATERAREADRAINED, l_mo.SRFCWATERCOMMUNITYCONFLAG,
           l_mo.UNMEASUREDTYPEACOUNT, l_mo.UNMEASUREDTYPEBCOUNT, l_mo.UNMEASUREDTYPECCOUNT, l_mo.UNMEASUREDTYPEDCOUNT, 
           l_mo.UNMEASUREDTYPEECOUNT, l_mo.UNMEASUREDTYPEFCOUNT, l_mo.UNMEASUREDTYPEGCOUNT, l_mo.UNMEASUREDTYPEHCOUNT, 
           l_mo.UNMEASUREDTYPEADESCRIPTION, l_mo.UNMEASUREDTYPEBDESCRIPTION, l_mo.UNMEASUREDTYPECDESCRIPTION, 
           l_mo.UNMEASUREDTYPEDDESCRIPTION, l_mo.UNMEASUREDTYPEEDESCRIPTION, l_mo.UNMEASUREDTYPEFDESCRIPTION, 
           l_mo.UNMEASUREDTYPEGDESCRIPTION, l_mo.UNMEASUREDTYPEHDESCRIPTION );
        EXCEPTION 
        WHEN DUP_VAL_ON_INDEX THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             
             IF (   t_prop(i).NO_TARIFF_GROUP <> 1 
                 OR t_prop(i).NO_TARIFF_SET   <> 1)
             THEN
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_exp := l_no_row_exp + 1;
             ELSE
                l_error_number := SQLCODE;
                l_error_message := SQLERRM;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', ' More than 1 tariff for Service Component',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_err := l_no_row_err + 1;
             END IF;
             
        WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_err := l_no_row_err + 1;
        END;
      

        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        ELSE 
            -- if tolearance limit has een exceeded, set error message and exit out
           IF (   l_no_row_exp > l_job.EXP_TOLERANCE
               OR l_no_row_err > l_job.ERR_TOLERANCE)   
           THEN
              CLOSE cur_prop; 
              l_job.IND_STATUS := 'ERR';
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
              COMMIT;
              return_code := -1;
              RETURN; 
           END IF;
           
        END IF;
         
        l_prev_prp := t_prop(i).NO_PROPERTY;
      
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
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1030, l_no_row_read,    'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1040, l_no_row_dropped, 'Dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1050, l_no_row_insert,  'Written to Table ');    

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
END P_MOU_TRAN_SERVICE_COMPONENT;
/
show errors;

exit;