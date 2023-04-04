Create or replace
PROCEDURE P_OWC_TRAN_SERVICE_COMPONENT(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                       no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                                       return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component MO Extract
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_OWC_TRAN_SERVICE_COMPONENT.sql
--
-- Subversion $Revision: 6292 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_SERVICE_COMPONENT from OWC supplied data 
--               - OWC_SERVICE_COMPONENT
--
-- NOTES  :
-- This package must be run each time the transform batch is run.
--
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-- V 0.02      21/09/2016  K.Burton   Added reconciliation measures per OWC
-- V 0.03      26/09/2016  K.Burton   Addional views and processing for WCA and SCA
-- V 0.04      10/10/2016  S.Badhan   Check on Tariff translation table for ST tariffs also.
-- V 0.05      18/10/2016  K.Burton   Removed reference to RECEPTION in OWC view queries
-- V 0.06      20/10/2016  K.Burton   Added TRIM to tariff lookup
-- V 0.07      27/10/2016  S.Badhan   I-370. Default assessed volume to 1 if required and not supplied
-- V 0.08      28/10/2016  S.Badhan   Set assessed band to null
-- V 0.09      17/11/2016  K.Burton   UPPER TARIFFCODE_PK - fix for settlements issue
-- V 0.10      18/11/2016  K.Burton   Back out above - need to accommodate little m's
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_SERVICE_COMPONENT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_prp                    MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE := 0;
  l_prev_tariff                 MO_TARIFF.TARIFFCODE_PK%TYPE;  
  l_SERVICECOMPONENTTYPE        MO_TARIFF.SERVICECOMPONENTTYPE%TYPE;    
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_sc                          LU_SERVICE_CATEGORY%ROWTYPE;
  l_tf                          LU_OWC_TARIFF%ROWTYPE;  
  l_mo                          MO_SERVICE_COMPONENT%ROWTYPE;
  l_sp                          MO_SUPPLY_POINT%ROWTYPE;
  l_mot                         MO_TARIFF%ROWTYPE;
  l_lu                          LU_SPID_RANGE%ROWTYPE;
  l_mpwt                        MO_TARIFF_TYPE_MPW%ROWTYPE;
  l_ast                         MO_TARIFF_TYPE_AS%ROWTYPE;
  l_awt                         MO_TARIFF_TYPE_AW%ROWTYPE;
  l_uwt                         MO_TARIFF_TYPE_UW%ROWTYPE;
  l_sw                          MO_TARIFF_TYPE_SW%ROWTYPE;
  l_mtv                         MO_TARIFF_VERSION%ROWTYPE;
  l_ust                         MO_TARIFF_TYPE_US%ROWTYPE;
  l_swt                         MO_SW_AREA_BAND%ROWTYPE;
  l_mwcap                       MO_MPW_STANDBY_MWCAPCHG%ROWTYPE;  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_sc_measure                  NUMBER;
  l_count                       NUMBER;
  l_band                        VARCHAR2(2);
  l_count_spids                 NUMBER:=0;
  l_no_serv_prov                NUMBER(9) := 0;  
  
  l_owc_measure                 LU_OWC_RECON_MEASURES%ROWTYPE;  
  l_no_row_read_as              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_as           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_as            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;

  l_no_row_read_aw              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_aw           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_aw            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_mpw             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_mpw          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_mpw           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;

  l_no_row_read_mnpw             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_mnpw          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_mnpw           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_ms              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_ms           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_ms            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_sw              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_sw           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_sw            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_hd              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_hd           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_hd            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_us              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_us           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_us            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_uw              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_uw           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_uw            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_wca             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_wca          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_wca           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  
  l_no_row_read_sca             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_sca          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_sca           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  
  l_tot_row_read                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_dropped             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_insert              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_as                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_aw                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_mpw                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_mnpw                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_hd                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_ms                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_tot_row_sw                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_tot_row_us                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_tot_row_uw                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;

  l_adjvol_pk NUMBER(9);
  l_servcomp_pk NUMBER(9);  

  CURSOR sc_cur (p_owc VARCHAR2) IS
    SELECT * FROM OWC_SERVICE_COMPONENT_TMP WHERE OWC = p_owc;
    
  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM RECEPTION.OWC_SERVICE_COMPONENT ORDER BY OWC;
    
  FUNCTION GET_OWC_MEASURES (p_owc VARCHAR2, p_table VARCHAR2) RETURN LU_OWC_RECON_MEASURES%ROWTYPE IS
    l_owc_measure LU_OWC_RECON_MEASURES%ROWTYPE;
  BEGIN
    SELECT * INTO l_owc_measure
    FROM LU_OWC_RECON_MEASURES 
    WHERE OWC = p_owc
    AND MO_TABLE = p_table;
    
    RETURN l_owc_measure;
  END GET_OWC_MEASURES;
  
  FUNCTION GET_TARIFF_MAPPING (p_sc_type VARCHAR2, p_owc VARCHAR2, p_tariffcode VARCHAR2) RETURN VARCHAR2 IS
    l_tariffcode VARCHAR2(50);
  BEGIN
    l_tariffcode := p_tariffcode;
    
    l_progress := 'SELECT LU_OWC_TARIFF - ' || p_sc_type;      
    BEGIN
      SELECT TRIM(STWTARIFFCODE_PK)
      INTO   l_tariffcode
      FROM   LU_OWC_TARIFF
      WHERE  WHOLESALERID_PK  = p_owc
      AND    TRIM(OWCTARIFFCODE_PK) = TRIM(p_tariffcode);
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
         l_tariffcode := NULL;
    END;

    IF l_tariffcode IS NOT NULL THEN
       l_progress := 'SELECT MO_TARIFF - ' || p_sc_type;      
       BEGIN
         SELECT SERVICECOMPONENTTYPE
         INTO   l_SERVICECOMPONENTTYPE
         FROM   MO_TARIFF
         WHERE  TARIFFCODE_PK = l_tariffcode
         AND    TARIFFSTATUS  = 'ACTIVE';
       EXCEPTION
       WHEN NO_DATA_FOUND THEN
            l_SERVICECOMPONENTTYPE := NULL;
       END;
    END IF; 

    RETURN l_tariffcode;
    
  END GET_TARIFF_MAPPING;
  
  PROCEDURE FN_ADD_DATA IS
  BEGIN

     l_no_serv_prov := l_no_serv_prov + 1;
     l_mo.SERVICECOMPONENTREF_PK := l_prev_prp || l_no_serv_prov;  

     l_progress := 'INSERT MO_SERVICE_COMPONENT';

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
         UNMEASUREDTYPEGDESCRIPTION, UNMEASUREDTYPEHDESCRIPTION, PIPESIZE, OWC)
         VALUES
         (l_mo.SERVICECOMPONENTREF_PK, l_mo.TARIFFCODE_PK, l_mo.SPID_PK, l_mo.DPID_PK, l_mo.STWPROPERTYNUMBER_PK, l_mo.STWSERVICETYPE, -- V.0.09/0.10
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
         l_mo.UNMEASUREDTYPEGDESCRIPTION, l_mo.UNMEASUREDTYPEHDESCRIPTION, l_mo.PIPESIZE, l_mo.OWC);
      EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Duplicate tariff Service Provision, record dropped',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
           l_no_row_war := l_no_row_war + 1;
       WHEN OTHERS THEN
           l_rec_written := FALSE;
           l_error_number := SQLCODE;
           l_error_message := SQLERRM;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
           l_no_row_err := l_no_row_err + 1;
      END;

      IF l_rec_written THEN
        CASE  l_mo.SERVICECOMPONENTTYPE
          WHEN 'MPW' THEN l_no_row_insert_mpw := l_no_row_insert_mpw + 1;
          WHEN 'MNPW' THEN l_no_row_insert_mnpw := l_no_row_insert_mnpw + 1;
          WHEN 'AW' THEN l_no_row_insert_aw := l_no_row_insert_aw + 1;
          WHEN 'AS' THEN l_no_row_insert_as := l_no_row_insert_as + 1;
          WHEN 'US' THEN l_no_row_insert_us := l_no_row_insert_us + 1;
          WHEN 'UW' THEN l_no_row_insert_uw := l_no_row_insert_uw + 1;
          WHEN 'SW' THEN l_no_row_insert_sw := l_no_row_insert_sw + 1;
          WHEN 'MS' THEN l_no_row_insert_ms := l_no_row_insert_ms + 1;
          WHEN 'HD' THEN l_no_row_insert_hd := l_no_row_insert_hd + 1;
          ELSE l_no_row_insert := l_no_row_insert + 1;
        END CASE;         
      ELSE
          -- if tolearance limit has been exceeded, set error message and exit out
        CASE  l_mo.SERVICECOMPONENTTYPE
          WHEN 'MPW' THEN l_no_row_dropped_mpw := l_no_row_dropped_mpw + 1;
          WHEN 'MNPW' THEN l_no_row_dropped_mnpw := l_no_row_dropped_mnpw + 1;
          WHEN 'AW' THEN l_no_row_dropped_aw := l_no_row_dropped_aw + 1;
          WHEN 'AS' THEN l_no_row_dropped_as := l_no_row_dropped_as + 1;
          WHEN 'US' THEN l_no_row_dropped_us := l_no_row_dropped_us + 1;
          WHEN 'UW' THEN l_no_row_dropped_uw := l_no_row_dropped_uw + 1;
          WHEN 'SW' THEN l_no_row_dropped_sw := l_no_row_dropped_sw + 1;
          WHEN 'MS' THEN l_no_row_dropped_ms := l_no_row_dropped_ms + 1;
          WHEN 'HD' THEN l_no_row_dropped_hd := l_no_row_dropped_hd + 1;
          ELSE l_no_row_dropped := l_no_row_dropped + 1;
        END CASE;
        
         IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE) THEN
            l_job.IND_STATUS := 'ERR';
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
            P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
            COMMIT;
            return_code := -1;
            RETURN;
         END IF;

      END IF;
  END FN_ADD_DATA;
  
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
   l_prev_tariff := NULL;

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

   l_progress := 'processing';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_MPW_V;
  
  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_MNPW_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_AW_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_AS_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_HD_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_SW_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_MS_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_US_V;

  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_UW_V;
  
  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_WCA_V;  
  
  INSERT INTO OWC_SERVICE_COMPONENT_TMP
  SELECT * FROM OWC_SERVICE_COMPONENT_SCA_V;
  
  -- start processing all records for range supplied
  FOR owc IN owc_cur
  LOOP
    -- reset counts for each OWC
    l_no_row_read    := 0;
    l_no_row_dropped := 0;
    l_no_row_insert  := 0;

    l_no_row_read_as              := 0;
    l_no_row_dropped_as           := 0;
    l_no_row_insert_as            := 0;
  
    l_no_row_read_aw              := 0;
    l_no_row_dropped_aw           := 0;
    l_no_row_insert_aw            := 0;
    
    l_no_row_read_mpw             := 0;
    l_no_row_dropped_mpw          := 0;
    l_no_row_insert_mpw           := 0;
  
    l_no_row_read_ms              := 0;
    l_no_row_dropped_ms           := 0;
    l_no_row_insert_ms            := 0;
    
    l_no_row_read_sw              := 0;
    l_no_row_dropped_sw           := 0;
    l_no_row_insert_sw            := 0;
    
    l_no_row_read_us              := 0;
    l_no_row_dropped_us           := 0;
    l_no_row_insert_us            := 0;
    
    l_no_row_read_uw              := 0;
    l_no_row_dropped_uw           := 0;
    l_no_row_insert_uw            := 0;
  
    l_progress := 'loop processing';
      FOR sc IN sc_cur(owc.OWC)
      LOOP
        l_mo := NULL;
        l_rec_written := TRUE;

        l_no_row_read := l_no_row_read + 1;
      
        l_err.TXT_KEY := owc.OWC || ',' || sc.SPID_PK;

        -- set values from cursor
        l_mo.SERVICECOMPONENTREF_PK := sc.SERVICECOMPONENTREF_PK; -- <== need to generate this somehow???
        l_mo.TARIFFCODE_PK := GET_TARIFF_MAPPING (sc.SERVICECOMPONENTTYPE, owc.OWC,sc.TARIFFCODE_PK);
        l_mo.SPID_PK := sc.SPID_PK;
        l_mo.DPID_PK := sc.DPID_PK;
        l_mo.STWPROPERTYNUMBER_PK := sc.STWPROPERTYNUMBER_PK;
        l_mo.STWSERVICETYPE := sc.STWSERVICETYPE;
        l_mo.SERVICECOMPONENTTYPE := sc.SERVICECOMPONENTTYPE;
        l_mo.SERVICECOMPONENTENABLED := sc.SERVICECOMPONENTENABLED;
        l_mo.EFFECTIVEFROMDATE := sc.EFFECTIVEFROMDATE;
        l_mo.SPECIALAGREEMENTFACTOR := sc.SPECIALAGREEMENTFACTOR;
        l_mo.SPECIALAGREEMENTFLAG := sc.SPECIALAGREEMENTFLAG;
        l_mo.SPECIALAGREEMENTREF := sc.SPECIALAGREEMENTREF;
        l_mo.METEREDFSMAXDAILYDEMAND := sc.METEREDFSMAXDAILYDEMAND;
        l_mo.METEREDPWMAXDAILYDEMAND := sc.METEREDPWMAXDAILYDEMAND;
        l_mo.METEREDNPWMAXDAILYDEMAND := sc.METEREDNPWMAXDAILYDEMAND;
        l_mo.METEREDFSDAILYRESVDCAPACITY := sc.METEREDFSDAILYRESVDCAPACITY;
        l_mo.METEREDNPWDAILYRESVDCAPACITY := sc.METEREDNPWDAILYRESVDCAPACITY;
        l_mo.DAILYRESERVEDCAPACITY := sc.DAILYRESERVEDCAPACITY;
        l_mo.HWAYSURFACEAREA := sc.HWAYSURFACEAREA;
        l_mo.HWAYCOMMUNITYCONFLAG := sc.HWAYCOMMUNITYCONFLAG;
        l_mo.ASSESSEDDVOLUMETRICRATE := sc.ASSESSEDDVOLUMETRICRATE;
        l_mo.ASSESSEDCHARGEMETERSIZE := sc.ASSESSEDCHARGEMETERSIZE;
        l_mo.ASSESSEDTARIFBAND := null;
        l_mo.SRFCWATERAREADRAINED := sc.SRFCWATERAREADRAINED;
        l_mo.SRFCWATERCOMMUNITYCONFLAG := sc.SRFCWATERCOMMUNITYCONFLAG;
        l_mo.UNMEASUREDTYPEACOUNT := sc.UNMEASUREDTYPEACOUNT;
        l_mo.UNMEASUREDTYPEBCOUNT := sc.UNMEASUREDTYPEBCOUNT;
        l_mo.UNMEASUREDTYPECCOUNT := sc.UNMEASUREDTYPECCOUNT;
        l_mo.UNMEASUREDTYPEDCOUNT := sc.UNMEASUREDTYPEDCOUNT;
        l_mo.UNMEASUREDTYPEECOUNT := sc.UNMEASUREDTYPEECOUNT;
        l_mo.UNMEASUREDTYPEFCOUNT := sc.UNMEASUREDTYPEFCOUNT;
        l_mo.UNMEASUREDTYPEGCOUNT := sc.UNMEASUREDTYPEGCOUNT;
        l_mo.UNMEASUREDTYPEHCOUNT := sc.UNMEASUREDTYPEHCOUNT;
        l_mo.UNMEASUREDTYPEADESCRIPTION := sc.UNMEASUREDTYPEADESCRIPTION;
        l_mo.UNMEASUREDTYPEBDESCRIPTION := sc.UNMEASUREDTYPEBDESCRIPTION;
        l_mo.UNMEASUREDTYPECDESCRIPTION := sc.UNMEASUREDTYPECDESCRIPTION;
        l_mo.UNMEASUREDTYPEDDESCRIPTION := sc.UNMEASUREDTYPEDDESCRIPTION;
        l_mo.UNMEASUREDTYPEEDESCRIPTION := sc.UNMEASUREDTYPEEDESCRIPTION;
        l_mo.UNMEASUREDTYPEFDESCRIPTION := sc.UNMEASUREDTYPEFDESCRIPTION;
        l_mo.UNMEASUREDTYPEGDESCRIPTION := sc.UNMEASUREDTYPEGDESCRIPTION;
        l_mo.UNMEASUREDTYPEHDESCRIPTION := sc.UNMEASUREDTYPEHDESCRIPTION;
        l_mo.PIPESIZE := sc.PIPESIZE;
        l_mo.OWC := owc.OWC;     
      
        -- Check if we have a valid tariff mapping - if not log an exception        
        IF l_mo.TARIFFCODE_PK IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC Tariff translation does not exist',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || sc.TARIFFCODE_PK,1,100));
          l_no_row_exp := l_no_row_exp + 1;
          l_rec_written := FALSE;  
        ELSIF NVL(l_SERVICECOMPONENTTYPE,'A') <> sc.SERVICECOMPONENTTYPE THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC Tariff incompatible with service component type',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || sc.TARIFFCODE_PK,1,100));
          l_no_row_exp := l_no_row_exp + 1;
          l_rec_written := FALSE;  
        END IF;    
        
        -- service component type specific processing
        CASE  sc.SERVICECOMPONENTTYPE
          WHEN 'MPW' THEN -- Metered Potable Water (MPW)
            l_no_row_read_mpw := l_no_row_read_mpw + 1;
            
            IF l_rec_written THEN
              -- Check Tariff values if max demand value required   
              l_progress := 'SELECT MO_TARIFF_VERSION';
              SELECT tv.TARIFF_VERSION_PK
              INTO   l_mtv.TARIFF_VERSION_PK
              FROM   MO_TARIFF tf,
                     MO_TARIFF_VERSION tv
              WHERE  tf.TARIFFCODE_PK = l_mo.TARIFFCODE_PK
              AND    tf.TARIFFCODE_PK = tv.TARIFFCODE_PK
              AND    tv.TARIFFVERSION = (SELECT MAX(TARIFFVERSION)
                                        FROM   MO_TARIFF_VERSION tv2
                                        WHERE  tv2.TARIFFCODE_PK = tv.TARIFFCODE_PK
                                        AND    tv2.TARIFFSTATUS  = 'ACTIVE');
                               
              l_progress := 'SELECT MO_TARIFF_TYPE_MPW';
              SELECT TARIFF_TYPE_PK,
                    MPWMAXIMUMDEMANDTARIFF
              INTO  l_mpwt.TARIFF_TYPE_PK,
                    l_mpwt.MPWMAXIMUMDEMANDTARIFF
              FROM  MO_TARIFF_TYPE_MPW
              WHERE TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;
            
              IF l_mpwt.MPWMAXIMUMDEMANDTARIFF IS NOT NULL THEN
                 IF l_mo.METEREDPWMAXDAILYDEMAND IS NULL THEN
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'METEREDPWMAXDAILYDEMAND has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                    l_no_row_exp := l_no_row_exp + 1;
                    l_rec_written := FALSE; 
                 END IF;
              END IF;
            
              -- check that for standby capacity charges we have provided mandatory single data    
              l_count := 1;
              BEGIN
                SELECT CHARGE
                INTO   l_mwcap.CHARGE
                FROM   MO_MPW_STANDBY_MWCAPCHG
                WHERE  TARIFF_TYPE_PK = l_mpwt.TARIFF_TYPE_PK
                AND    ROWNUM = 1;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                   l_count := 0;
              END;
            
              IF l_count > 0 THEN
                 IF l_mo.DAILYRESERVEDCAPACITY IS NULL THEN
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'DAILYRESERVEDCAPACITY has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                    l_no_row_exp := l_no_row_exp + 1;
                    l_rec_written := FALSE;
                 END IF;
              END IF;
            END IF;
            
            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_mpw := l_no_row_dropped_mpw + 1; 
            END IF;
          WHEN 'MNPW' THEN -- Metered Non Potable Water (MNPW)
            l_no_row_read_mnpw := l_no_row_read_mnpw + 1;

            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_mnpw := l_no_row_dropped_mnpw + 1;
            END IF;            
          WHEN 'AW' THEN -- Assessed Water(AW)
            l_no_row_read_aw := l_no_row_read_aw + 1;

            IF l_rec_written THEN
              l_progress := 'SELECT MO_TARIFF_VERSION';
              SELECT tv.TARIFF_VERSION_PK
              INTO   l_mtv.TARIFF_VERSION_PK
              FROM   MO_TARIFF tf,
                    MO_TARIFF_VERSION tv
              WHERE  tf.TARIFFCODE_PK = l_mo.TARIFFCODE_PK
              AND    tf.TARIFFCODE_PK = tv.TARIFFCODE_PK
              AND    tv.TARIFFVERSION = (SELECT MAX(TARIFFVERSION)
                                        FROM   MO_TARIFF_VERSION tv2
                                        WHERE  tv2.TARIFFCODE_PK = tv.TARIFFCODE_PK
                                        AND    tv2.TARIFFSTATUS  = 'ACTIVE');
    
              -- check Tariff if volumetric rate required
              l_progress := 'SELECT MO_TARIFF_TYPE_AW';
              SELECT TARIFF_TYPE_PK,
                    AWFIXEDCHARGE,
                    AWVOLUMETRICCHARGE
              INTO   l_awt.TARIFF_TYPE_PK,
                    l_awt.AWFIXEDCHARGE,
                    l_awt.AWVOLUMETRICCHARGE
              FROM   MO_TARIFF_TYPE_AW
              WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;
    
              IF nvl(l_awt.AWVOLUMETRICCHARGE,0) <> 0 THEN
                IF nvl(l_mo.ASSESSEDDVOLUMETRICRATE,0) = 0 THEN
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'OWC ASSESSEDDVOLUMETRICRATE has no value, defaulted to 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_war := l_no_row_war + 1;
                   l_mo.ASSESSEDDVOLUMETRICRATE := 1;
                END IF;
              END IF;
            END IF;

            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_aw := l_no_row_dropped_aw + 1;  
            END IF;
          WHEN 'AS' THEN -- Assessed Sewage(AS)
            l_no_row_read_as := l_no_row_read_as + 1;
            
            IF l_rec_written THEN
              l_progress := 'SELECT MO_TARIFF_VERSION';
              SELECT tv.TARIFF_VERSION_PK
              INTO   l_mtv.TARIFF_VERSION_PK
              FROM   MO_TARIFF tf,
                    MO_TARIFF_VERSION tv
              WHERE  tf.TARIFFCODE_PK = l_mo.TARIFFCODE_PK
              AND    tf.TARIFFCODE_PK = tv.TARIFFCODE_PK
              AND    tv.TARIFFVERSION = (SELECT MAX(TARIFFVERSION)
                                        FROM   MO_TARIFF_VERSION tv2
                                        WHERE  tv2.TARIFFCODE_PK = tv.TARIFFCODE_PK
                                        AND    tv2.TARIFFSTATUS  = 'ACTIVE');
    
             -- check tariff if volume required
              l_progress := 'SELECT MO_TARIFF_TYPE_AS';
              SELECT TARIFF_TYPE_PK,
                     ASFIXEDCHARGE,
                     ASVOLMETCHARGE
              INTO   l_ast.TARIFF_TYPE_PK,
                     l_ast.ASFIXEDCHARGE,
                     l_ast.ASVOLMETCHARGE
              FROM   MO_TARIFF_TYPE_AS
              WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;
    
              IF nvl(l_ast.ASVOLMETCHARGE,0) <> 0 THEN
                IF nvl(l_mo.ASSESSEDDVOLUMETRICRATE,0) = 0 THEN
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'OWC ASSESSEDDVOLUMETRICRATE has no value, defaulted to 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_war := l_no_row_war + 1;
                   l_mo.ASSESSEDDVOLUMETRICRATE := 1;
                END IF;
              END IF;
            END IF;

            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_as := l_no_row_dropped_as + 1;
            END IF;
          WHEN 'US' THEN -- Unmeasured Sewage(US)
            l_no_row_read_us := l_no_row_read_us + 1;

            IF l_rec_written THEN
              IF nvl(l_mo.PIPESIZE,1) <> 0 THEN
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'PIPESIZE must be 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                l_no_row_war := l_no_row_war + 1;
                l_mo.PIPESIZE := 0;
              END IF;
            END IF;
          
            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_us := l_no_row_dropped_us + 1;
            END IF;
          WHEN 'UW' THEN -- Unmeasured Water(UW)
            l_no_row_read_uw := l_no_row_read_uw + 1;
            
            IF l_rec_written THEN
              IF nvl(l_mo.PIPESIZE,1) <> 0 THEN
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'PIPESIZE must be 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                 l_no_row_war := l_no_row_war + 1;
                 l_mo.PIPESIZE := 0;
              END IF;
            END IF;
            
            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_uw := l_no_row_dropped_uw + 1;
            END IF;            
          WHEN 'SW' THEN -- Surface Water (SW)
            l_no_row_read_sw := l_no_row_read_sw + 1;

            IF l_rec_written THEN
              -- check if tariff has a rateable value
              l_progress := 'SELECT MO_TARIFF_VERSION';
              SELECT tv.TARIFF_VERSION_PK
              INTO   l_mtv.TARIFF_VERSION_PK
              FROM   MO_TARIFF tf,
                    MO_TARIFF_VERSION tv
              WHERE  tf.TARIFFCODE_PK = l_mo.TARIFFCODE_PK
              AND    tf.TARIFFCODE_PK = tv.TARIFFCODE_PK
              AND    tv.TARIFFVERSION = (SELECT MAX(TARIFFVERSION)
                                        FROM   MO_TARIFF_VERSION tv2
                                        WHERE  tv2.TARIFFCODE_PK = tv.TARIFFCODE_PK
                                        AND    tv2.TARIFFSTATUS  = 'ACTIVE');
      
              l_progress := 'SELECT MO_TARIFF_TYPE_SW';
              SELECT TARIFF_TYPE_PK,
                    SWFIXEDCHARGE,
                    SWRVPOUNDAGE
              INTO   l_sw.TARIFF_TYPE_PK,
                    l_sw.SWFIXEDCHARGE,
                    l_sw.SWRVPOUNDAGE
              FROM   MO_TARIFF_TYPE_SW sw
              WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;
  
              IF nvl(l_sw.SWRVPOUNDAGE,0)  > 0 THEN
                l_mo.SRFCWATERAREADRAINED	:= 0;
              END IF;
  
              -- no rateable charge then get band charge
              IF nvl(l_sw.SWRVPOUNDAGE,0)  = 0 THEN
                l_progress := 'SELECT MO_SW_AREA_BAND';
  
                l_band := substr(l_mo.TARIFFCODE_PK, LENGTH(l_mo.TARIFFCODE_PK) - 1, 2);
                l_swt.BAND := NULL;
  
                IF LENGTH(TRIM(TRANSLATE(l_band, ' +-.0123456789', ' '))) IS NULL THEN
                  BEGIN
                    SELECT BAND,LOWERAREA,UPPERAREA
                    INTO   l_swt.BAND,
                           l_swt.LOWERAREA,
                           l_swt.UPPERAREA
                    FROM   MO_SW_AREA_BAND
                    WHERE  TARIFF_TYPE_PK = l_sw.TARIFF_TYPE_PK
                    AND    BAND           = l_band;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      l_swt.BAND := NULL;
                  END;
                END IF;
  
                IF l_swt.BAND IS NULL THEN
                  IF nvl(l_sw.SWFIXEDCHARGE,0)  = 0 THEN
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'SRFCWATERAREADRAINED has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                    l_no_row_war := l_no_row_war + 1;
                  END IF;
                  l_mo.SRFCWATERAREADRAINED := 0;
                ELSE
                  IF l_swt.UPPERAREA IS NOT NULL THEN
                    l_mo.SRFCWATERAREADRAINED := ((l_swt.UPPERAREA - l_swt.LOWERAREA )/2) + l_swt.LOWERAREA;
                  ELSE
                    l_mo.SRFCWATERAREADRAINED := l_swt.LOWERAREA;
                  END IF;
                END IF;
              END IF;
            END IF;

            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_sw := l_no_row_dropped_sw + 1; 
            END IF;            
          WHEN 'MS' THEN -- Metered Foul Sewage (MS)
            l_no_row_read_ms := l_no_row_read_ms + 1;
            
            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_ms := l_no_row_dropped_ms + 1;
            END IF;
          WHEN 'HD' THEN -- Highway Drainage (HD)
            l_no_row_read_hd := l_no_row_read_hd + 1;
            
            IF l_rec_written THEN
              FN_ADD_DATA;
            ELSE
              l_no_row_dropped_hd := l_no_row_dropped_hd + 1;
            END IF;
          WHEN 'WCA' THEN -- Water Charge Adjustment (WCA)
            l_no_row_read_wca := l_no_row_read_wca + 1;
            
            IF l_rec_written THEN
              BEGIN
                SELECT SERVICECOMPONENTREF_PK
                INTO l_servcomp_pk
                FROM MO_SERVICE_COMPONENT
                WHERE SPID_PK = l_mo.SPID_PK;
                
                SELECT NVL(MAX(ADJUSTMENTSVOLADJUNIQREF_PK)+1,1) 
                INTO l_adjvol_pk
                FROM MO_SERVICE_COMPONENT_VOL_ADJ;
                
                INSERT INTO MO_SERVICE_COMPONENT_VOL_ADJ (ADJUSTMENTSVOLADJUNIQREF_PK,SERVICECOMPONENTREF_PK,ADJUSTMENTSCHARGEADJTARIFFCODE)
                VALUES (l_adjvol_pk,l_servcomp_pk,l_mo.TARIFFCODE_PK);
                
                l_no_row_insert_wca := l_no_row_insert_wca + 1;
              EXCEPTION
                WHEN OTHERS THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error loading Water Charge Adjustment',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_exp := l_no_row_exp + 1;
                  l_rec_written := FALSE;
                  l_no_row_dropped_wca := l_no_row_dropped_wca + 1;
              END;
            ELSE
              l_no_row_dropped_wca := l_no_row_dropped_wca + 1;
            END IF;
          WHEN 'SCA' THEN -- Sewerage Charge Adjustment (SCA)
            l_no_row_read_sca := l_no_row_read_sca + 1;
            
            IF l_rec_written THEN
              BEGIN
                SELECT SERVICECOMPONENTREF_PK
                INTO l_servcomp_pk
                FROM MO_SERVICE_COMPONENT
                WHERE SPID_PK = l_mo.SPID_PK;
                
                SELECT NVL(MAX(ADJUSTMENTSVOLADJUNIQREF_PK)+1,1) 
                INTO l_adjvol_pk
                FROM MO_SERVICE_COMPONENT_VOL_ADJ;
                
                INSERT INTO MO_SERVICE_COMPONENT_VOL_ADJ (ADJUSTMENTSVOLADJUNIQREF_PK,SERVICECOMPONENTREF_PK,ADJUSTMENTSCHARGEADJTARIFFCODE)
                VALUES (l_adjvol_pk,l_servcomp_pk,l_mo.TARIFFCODE_PK);

                l_no_row_insert_sca := l_no_row_insert_sca + 1;
              EXCEPTION
                WHEN OTHERS THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error loading Water Charge Adjustment',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_exp := l_no_row_exp + 1;
                  l_rec_written := FALSE;
                  l_no_row_dropped_wca := l_no_row_dropped_wca + 1;
              END;
            ELSE
              l_no_row_dropped_sca := l_no_row_dropped_sca + 1;
            END IF;
          ELSE 
            l_no_row_read := l_no_row_read + 1;
        END CASE;
    END LOOP; --sc_cur

    COMMIT;

    -- write OWC specific counts 
    l_progress := 'Writing OWC counts ' || owc.OWC;  
    
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_MPW');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_mpw, owc.OWC || ' MPW Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_mpw, owc.OWC || ' MPW Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_mpw, owc.OWC || ' MPW Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_MNPW');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_mnpw, owc.OWC || ' MNPW Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_mnpw, owc.OWC || ' MNPW Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_mnpw, owc.OWC || ' MNPW Service Components to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_AW');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_aw, owc.OWC || ' AW Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_aw, owc.OWC || ' AW Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_aw, owc.OWC || ' AW Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_UW');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_uw, owc.OWC || ' UW Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_uw, owc.OWC || ' UW Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_uw, owc.OWC || ' UW Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_MS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_ms, owc.OWC || ' MS Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_ms, owc.OWC || ' MS Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_ms, owc.OWC || ' MS Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_AS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_as, owc.OWC || ' AS Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_as, owc.OWC || ' AS Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_as, owc.OWC || ' AS Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_US');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_us, owc.OWC || ' US Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_us, owc.OWC || ' US Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_us, owc.OWC || ' US Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_SW');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_sw, owc.OWC || ' SW Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_sw, owc.OWC || ' SW Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_sw, owc.OWC || ' SW Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SERVICE_COMPONENT_HD');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_hd, owc.OWC || ' HD Service Components read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_hd, owc.OWC || ' HD Service Components dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_hd, owc.OWC || ' HD Service Components written to MO_SERVICE_COMPONENT during Transform'); 

    -- maintain total counts
    l_tot_row_read := l_tot_row_read + l_no_row_read_mpw + l_no_row_read_mnpw + l_no_row_read_aw + l_no_row_read_uw + l_no_row_read_ms + l_no_row_read_as + l_no_row_read_us + l_no_row_read_sw + l_no_row_read_hd;
    l_tot_row_dropped := l_tot_row_dropped + l_no_row_dropped_mpw + l_no_row_dropped_mnpw + l_no_row_dropped_aw + l_no_row_dropped_uw + l_no_row_dropped_ms + l_no_row_dropped_as + l_no_row_dropped_us + l_no_row_dropped_sw + l_no_row_dropped_hd;
    l_tot_row_insert := l_tot_row_insert + l_no_row_insert_mpw + l_no_row_insert_mnpw + l_no_row_insert_aw + l_no_row_insert_uw + l_no_row_insert_ms + l_no_row_insert_as + l_no_row_insert_us + l_no_row_insert_sw + l_no_row_insert_hd;

    l_tot_row_mpw := l_tot_row_mpw + l_no_row_read_mpw;
    l_tot_row_as := l_tot_row_as + l_no_row_read_as;
    l_tot_row_aw := l_tot_row_aw + l_no_row_read_aw;
    l_tot_row_ms := l_tot_row_ms + l_no_row_read_ms;
    l_tot_row_sw := l_tot_row_sw + l_no_row_read_sw;
    l_tot_row_us := l_tot_row_us + l_no_row_read_us;
    l_tot_row_uw := l_tot_row_uw + l_no_row_read_uw;    
    l_tot_row_hd := l_tot_row_hd + l_no_row_read_hd;    
  END LOOP; -- owc_cur
  
  DELETE FROM OWC_SERVICE_COMPONENT_TMP;
      
      
  -- write counts
  l_progress := 'Writing Total Counts';

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1030, l_tot_row_read,    'OWC Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1040, l_tot_row_dropped, 'OWC Dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1050, l_tot_row_insert,  'OWC Written to Table ');
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 700, l_tot_row_mpw, 'OWC Distinct Service Component Type MPW');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 710, l_tot_row_mnpw,'OWC Distinct Service Component Type MNPW');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 750, l_tot_row_as,  'OWC Distinct Service Component Type AS');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 720, l_tot_row_aw,  'OWC Distinct Service Component Type AW');  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 740, l_tot_row_ms,  'OWC Distinct Service Component Type MS');    
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 770, l_tot_row_sw,  'OWC Distinct Service Component Type SW');    
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 760, l_tot_row_us,  'OWC Distinct Service Component Type US');    
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 730, l_tot_row_uw,  'OWC Distinct Service Component Type UW');    
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 780, l_tot_row_hd,  'OWC Distinct Service Component Type HD');    

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
       RETURN_CODE := -1;
END P_OWC_TRAN_SERVICE_COMPONENT;
/
show errors;

exit;