create or replace
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
-- Subversion $Revision: 5458 $
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
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_SERVICE_COMPONENT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_prp                    MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE := 0;
  l_prev_tariff                 MO_TARIFF.TARIFFCODE_PK%TYPE;  
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_sc                          LU_SERVICE_CATEGORY%ROWTYPE;
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
  l_no_row_as                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_aw                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_mpw                  MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_ms                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_sw                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_no_row_us                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_no_row_uw                   MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;

CURSOR cur_prop (p_property_start   MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_property_end     MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE)
    IS
     SELECT sc.SPID_PK,
            sc.METEREDPWTARIFFCODE,
            sc.MPWSPECIALAGREEMENTFLAG,
            sc.MPWSPECIALAGREEMENTFACTOR,
            sc.MPWSPECIALAGREEMENTREF,
            sc.METEREDPWMAXDAILYDEMAND,
            sc.MPWMAXIMUMDEMANDTARIFF,
            sc.DAILYRESERVEDCAPACITY,
            sc.MPWDAILYSTANDBYUSAGEVOLCHARGE,
            sc.METEREDNPWTARIFFCODE,
            sc.MNPWSPECIALAGREEMENTFLAG,
            sc.MNPWSPECIALAGREEMENTFACTOR,
            sc.MNPWSPECIALAGREEMENTREF,
            sc.METEREDNPWMAXDAILYDEMAND,
            sc.MNPWMAXIMUMDEMANDTARIFF,
            sc.METEREDNPWDAILYRESVDCAPACITY,
            sc.MNPWDAILYSTANDBYUSAGEVOLCHARGE,
            sc.AWASSESSEDTARIFFCODE,
            sc.AWSPECIALAGREEMENTFLAG,
            sc.AWSPECIALAGREEMENTFACTOR,
            sc.AWSPECIALAGREEMENTREF,
            sc.AWASSESSEDCHARGEMETERSIZE,
            sc.AWASSESSEDDVOLUMETRICRATE,
            sc.AWASSESSEDTARIFBAND,
            sc.AWFIXEDCHARGE,
            sc.AWVOLUMETRICCHARGE,
            sc.AWTARIFFBAND,
            sc.UWUNMEASUREDTARIFFCODE,
            sc.UWSPECIALAGREEMENTFLAG,
            sc.UWSPECIALAGREEMENTFACTOR,
            sc.UWSPECIALAGREEMENTREF,
            sc.UWUNMEASUREDTYPEACOUNT,
            sc.UWUNMEASUREDTYPEADESCRIPTION,
            sc.UWUNMEASUREDTYPEBCOUNT,
            sc.UWUNMEASUREDTYPEBDESCRIPTION,
            sc.UWUNMEASUREDTYPECCOUNT,
            sc.UWUNMEASUREDTYPECDESCRIPTION,
            sc.UWUNMEASUREDTYPEDCOUNT,
            sc.UWUNMEASUREDTYPEDDESCRIPTION,
            sc.UWUNMEASUREDTYPEECOUNT,
            sc.UWUNMEASUREDTYPEEDESCRIPTION,
            sc.UWUNMEASUREDTYPEFCOUNT,
            sc.UWUNMEASUREDTYPEFDESCRIPTION,
            sc.UWUNMEASUREDTYPEGCOUNT,
            sc.UWUNMEASUREDTYPEGDESCRIPTION,
            sc.UWUNMEASUREDTYPEHCOUNT,
            sc.UWUNMEASUREDTYPEHDESCRIPTION,
            sc.UWPIPESIZE,
            sc.WADJCHARGEADJTARIFFCODE,
            sc.METEREDFSTARIFFCODE,
            sc.MFSSPECIALAGREEMENTFLAG,
            sc.MFSSPECIALAGREEMENTFACTOR,
            sc.MFSSPECIALAGREEMENTREF,
            sc.ASASSESSEDTARIFFCODE,
            sc.ASSPECIALAGREEMENTFLAG,
            sc.ASSPECIALAGREEMENTFACTOR,
            sc.ASSPECIALAGREEMENTREF,
            sc.ASASSESSEDCHARGEMETERSIZE,
            sc.ASASSESSEDDVOLUMETRICRATE,
            sc.ASASSESSEDTARIFBAND,
            sc.ASFIXEDCHARGE,
            sc.ASVOLMETCHARGE,
            sc.ASTARIFFBAND,
            sc.USUNMEASUREDTARIFFCODE,
            sc.USSPECIALAGREEMENTFLAG,
            sc.USSPECIALAGREEMENTFACTOR,
            sc.USSPECIALAGREEMENTREF,
            sc.USUNMEASUREDTYPEACOUNT,
            sc.USUNMEASUREDTYPEADESCRIPTION,
            sc.USUNMEASUREDTYPEBCOUNT,
            sc.USUNMEASUREDTYPEBDESCRIPTION,
            sc.USUNMEASUREDTYPECCOUNT,
            sc.USUNMEASUREDTYPECDESCRIPTION,
            sc.USUNMEASUREDTYPEDCOUNT,
            sc.USUNMEASUREDTYPEDDESCRIPTION,
            sc.USUNMEASUREDTYPEECOUNT,
            sc.USUNMEASUREDTYPEEDESCRIPTION,
            sc.USUNMEASUREDTYPEFCOUNT,
            sc.USUNMEASUREDTYPEFDESCRIPTION,
            sc.USUNMEASUREDTYPEGCOUNT,
            sc.USUNMEASUREDTYPEGDESCRIPTION,
            sc.USUNMEASUREDTYPEHCOUNT,
            sc.USUNMEASUREDTYPEHDESCRIPTION,
            sc.USPIPESIZE,
            sc.SADJCHARGEADJTARIFFCODE,
            sc.SRFCWATERTARRIFCODE,
            sc.SRFCWATERAREADRAINED,
            sc.SRFCWATERCOMMUNITYCONFLAG,
            sc.SWSPECIALAGREEMENTFLAG,
            sc.SWSPECIALAGREEMENTFACTOR,
            sc.SWSPECIALAGREEMENTREF,
            sc.HWAYDRAINAGETARIFFCODE,
            sc.HWAYSURFACEAREA,
            sc.HWAYCOMMUNITYCONFLAG,
            sc.HDSPECIALAGREEMENTFLAG,
            sc.HDSPECIALAGREEMENTFACTOR,
            sc.HDSPECIALAGREEMENTREF,
            sc.SAPFLOCNUMBER,
            sc.OWC,
            pr.STWPROPERTYNUMBER_PK
    FROM    RECEPTION.SAP_SERVICE_COMPONENT sc 
            LEFT JOIN MO_ELIGIBLE_PREMISES pr ON pr.CORESPID_PK = substr(sc.SPID_PK,1,10) 
    WHERE   pr.STWPROPERTYNUMBER_PK BETWEEN p_property_start AND p_property_end  -- and pr.STWPROPERTYNUMBER_PK = 225002539
    order by pr.STWPROPERTYNUMBER_PK; 


TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;

  PROCEDURE FN_ADD_DATA
  IS
  BEGIN

     l_no_serv_prov := l_no_serv_prov + 1;
     l_mo.SERVICECOMPONENTREF_PK := l_prev_prp || l_no_serv_prov;  

      -- If cross border tariff check wholesaler id is set

      IF l_rec_written THEN
         IF substr(l_mo.TARIFFCODE_PK,1,4) <> '1STW' THEN

            l_progress := 'SELECT MO_SUPPLY_POINT';
            BEGIN
              SELECT WHOLESALERID_PK
              INTO   l_sp.WHOLESALERID_PK
              FROM   MO_SUPPLY_POINT
              WHERE  SPID_PK = l_mo.SPID_PK;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Supply Point does not exist',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                 l_no_row_exp := l_no_row_exp + 1;
                 l_rec_written := FALSE;
            END;

            IF l_rec_written THEN 
              CASE   substr(l_mo.TARIFFCODE_PK,1,4)
                WHEN '1ANG' THEN
                      IF l_sp.WHOLESALERID_PK <> 'ANGLIAN-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
                WHEN '1NWE' THEN
                      IF l_sp.WHOLESALERID_PK <> 'UNITED-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
                WHEN '1THA' THEN
                      IF l_sp.WHOLESALERID_PK <> 'THAMES-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               WHEN '1WEL' THEN
                      IF l_sp.WHOLESALERID_PK <> 'DWRCYMRU-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               WHEN '1YOR' THEN
                      IF l_sp.WHOLESALERID_PK <> 'YORKSHIRE-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               ELSE
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'UNKNOWN TARIFF WATER COMPANY',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_exp := l_no_row_exp + 1;
                   l_rec_written := FALSE;
              END CASE;
           END IF;   
         END IF;
      END IF;

     l_progress := 'INSERT MO_SERVICE_COMPONENT';

     IF l_rec_written THEN
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
           UNMEASUREDTYPEGDESCRIPTION, UNMEASUREDTYPEHDESCRIPTION, PIPESIZE)
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
           l_mo.UNMEASUREDTYPEGDESCRIPTION, l_mo.UNMEASUREDTYPEHDESCRIPTION, l_mo.PIPESIZE);
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
      END IF;

      IF l_rec_written THEN
         l_no_row_insert := l_no_row_insert + 1;
      ELSE
          -- if tolearance limit has een exceeded, set error message and exit out
         l_no_row_dropped := l_no_row_dropped + 1;    
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

  -- start processing all records for range supplied

  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing';

  LOOP

    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_prop.COUNT
    LOOP

      l_err.TXT_KEY := t_prop(i).STWPROPERTYNUMBER_PK || ',' || t_prop(i).SPID_PK;

      l_mo := NULL;
      l_rec_written := TRUE;

      -- keep count of distinct property
      l_no_row_read := l_no_row_read + 1;

      l_mo.SPID_PK := t_prop(i).SPID_PK;
      l_mo.DPID_PK := null;
      l_mo.STWPROPERTYNUMBER_PK := t_prop(i).STWPROPERTYNUMBER_PK;
      l_mo.SERVICECOMPONENTENABLED := 1;    --- *********** fix

      l_mo.STWSERVICETYPE	:= NULL;         


      SELECT MAX(NVL(SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')))
      INTO  l_sp.SUPPLYPOINTEFFECTIVEFROMDATE
      FROM  MO_SUPPLY_POINT
      WHERE SPID_PK = l_mo.SPID_PK;
      
      l_mo.EFFECTIVEFROMDATE := l_sp.SUPPLYPOINTEFFECTIVEFROMDATE;
      l_mo.SRFCWATERCOMMUNITYCONFLAG := 0;
      l_mo.HWAYCOMMUNITYCONFLAG := 0;
      l_mo.SRFCWATERAREADRAINED	:= 0;

      IF l_prev_prp <> t_prop(i).STWPROPERTYNUMBER_PK THEN
         l_no_serv_prov := 0;
         l_prev_prp := t_prop(i).STWPROPERTYNUMBER_PK;
      END IF;

--     -- Get Tariff details
--
--     l_progress := 'SELECT MO_TARIFF';
--     BEGIN
--        SELECT SERVICECOMPONENTTYPE
--        INTO   l_mot.SERVICECOMPONENTTYPE
--        FROM   MO_TARIFF
--        WHERE  TARIFFCODE_PK = l_mo.TARIFFCODE_PK;
--     EXCEPTION
--     WHEN NO_DATA_FOUND THEN
--          l_mot.SERVICECOMPONENTTYPE := NULL;
--          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
--          l_no_row_exp := l_no_row_exp + 1;
--          l_rec_written := FALSE;
--     END;


      -- if water company Severn Trent must exist on lookup file.
      IF t_prop(i).OWC = 'SEVERN' THEN 
         l_progress := 'SELECT LU_SPID_RANGE';      
         BEGIN 
            SELECT SPID_PK 
            INTO  l_lu.SPID_PK
            FROM  LU_SPID_RANGE
            WHERE SPID_PK = t_prop(i).SPID_PK;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Severn Trent SPID not in SPID range',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_exp := l_no_row_exp + 1;
         END;
      END IF;


      -- Metered Potable Water (MPW)
      
      IF (    t_prop(i).METEREDPWTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'MPW';
         l_no_row_mpw := l_no_row_mpw + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).METEREDPWTARIFFCODE;
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).MPWSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).MPWSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).MPWSPECIALAGREEMENTREF;
         l_mo.METEREDPWMAXDAILYDEMAND := t_prop(i).METEREDPWMAXDAILYDEMAND;
         l_mo.DAILYRESERVEDCAPACITY := t_prop(i).DAILYRESERVEDCAPACITY;   

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
         INTO   l_mpwt.TARIFF_TYPE_PK,
                l_mpwt.MPWMAXIMUMDEMANDTARIFF
         FROM   MO_TARIFF_TYPE_MPW
         WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;
      
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

         IF l_rec_written THEN
            FN_ADD_DATA;
         END IF;

       END IF;
       l_mo.SPECIALAGREEMENTFLAG := null;
       l_mo.SPECIALAGREEMENTFACTOR := null;
       l_mo.SPECIALAGREEMENTREF := null;
       l_mo.METEREDPWMAXDAILYDEMAND := NULL;
       l_mo.DAILYRESERVEDCAPACITY := null;

      -- Metered Non Potable Water (MNPW)

      IF (    t_prop(i).METEREDNPWTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'MNPW';
         l_mo.TARIFFCODE_PK := t_prop(i).METEREDNPWTARIFFCODE;
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).MNPWSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).MNPWSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).MNPWSPECIALAGREEMENTREF;      
         l_mo.METEREDNPWMAXDAILYDEMAND	:= t_prop(i).METEREDNPWMAXDAILYDEMAND;
         l_mo.METEREDNPWDAILYRESVDCAPACITY := t_prop(i).METEREDNPWDAILYRESVDCAPACITY;
         FN_ADD_DATA;
      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := null;
      l_mo.METEREDNPWMAXDAILYDEMAND	:= null;
      l_mo.METEREDNPWDAILYRESVDCAPACITY := null;

      -- Assessed Water(AW)   
      IF (    t_prop(i).AWASSESSEDTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'AW';
         l_no_row_aw := l_no_row_aw + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).AWASSESSEDTARIFFCODE;
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).AWSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).AWSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).AWSPECIALAGREEMENTREF;
         l_mo.ASSESSEDDVOLUMETRICRATE	:= t_prop(i).AWASSESSEDDVOLUMETRICRATE;
         l_mo.ASSESSEDCHARGEMETERSIZE := t_prop(i).AWASSESSEDCHARGEMETERSIZE;
         l_mo.ASSESSEDTARIFBAND := t_prop(i).AWASSESSEDTARIFBAND;

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
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'ASSESSEDDVOLUMETRICRATE has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
               l_no_row_exp := l_no_row_exp + 1;
               l_rec_written := FALSE;
            END IF;
         END IF;

         IF l_rec_written THEN
            FN_ADD_DATA;
         END IF;

      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := null;
      l_mo.ASSESSEDDVOLUMETRICRATE	:= null;
      l_mo.ASSESSEDCHARGEMETERSIZE := NULL;
      l_mo.ASSESSEDTARIFBAND := NULL;

      -- Unmeasured Water(UW)
      IF (    t_prop(i).UWUNMEASUREDTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'UW';
         l_no_row_uw := l_no_row_uw + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).UWUNMEASUREDTARIFFCODE;
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).UWSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).UWSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).UWSPECIALAGREEMENTREF;   
         l_mo.UNMEASUREDTYPEBCOUNT := t_prop(i).UWUNMEASUREDTYPEBCOUNT;
         l_mo.UNMEASUREDTYPECCOUNT := t_prop(i).UWUNMEASUREDTYPECCOUNT;
         l_mo.UNMEASUREDTYPEDCOUNT	:= t_prop(i).UWUNMEASUREDTYPEDCOUNT;
         l_mo.UNMEASUREDTYPEECOUNT := t_prop(i).UWUNMEASUREDTYPEECOUNT;
         l_mo.UNMEASUREDTYPEFCOUNT := t_prop(i).UWUNMEASUREDTYPEFCOUNT;
         l_mo.UNMEASUREDTYPEGCOUNT := t_prop(i).UWUNMEASUREDTYPEGCOUNT;
         l_mo.UNMEASUREDTYPEHCOUNT := t_prop(i).UWUNMEASUREDTYPEHCOUNT;
         l_mo.UNMEASUREDTYPEADESCRIPTION := t_prop(i).UWUNMEASUREDTYPEADESCRIPTION;
         l_mo.UNMEASUREDTYPEBDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEBDESCRIPTION;
         l_mo.UNMEASUREDTYPECDESCRIPTION := t_prop(i).UWUNMEASUREDTYPECDESCRIPTION;
         l_mo.UNMEASUREDTYPEDDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEDDESCRIPTION;
         l_mo.UNMEASUREDTYPEEDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEEDESCRIPTION;
         l_mo.UNMEASUREDTYPEFDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEFDESCRIPTION;
         l_mo.UNMEASUREDTYPEGDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEGDESCRIPTION;
         l_mo.UNMEASUREDTYPEHDESCRIPTION := t_prop(i).UWUNMEASUREDTYPEHDESCRIPTION;
         l_mo.PIPESIZE := t_prop(i).UWPIPESIZE;
         
         IF nvl(l_mo.PIPESIZE,1) <> 0 THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'PIPESIZE must be 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
         ELSE 
            FN_ADD_DATA;
         END IF;
         
      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := NULL;
      l_mo.UNMEASUREDTYPEBCOUNT := null;
      l_mo.UNMEASUREDTYPECCOUNT := null;
      l_mo.UNMEASUREDTYPEDCOUNT	:= null;
      l_mo.UNMEASUREDTYPEECOUNT := null;
      l_mo.UNMEASUREDTYPEFCOUNT := null;
      l_mo.UNMEASUREDTYPEGCOUNT := NULL;
      l_mo.UNMEASUREDTYPEHCOUNT := null;
      l_mo.UNMEASUREDTYPEADESCRIPTION := null;
      l_mo.UNMEASUREDTYPEBDESCRIPTION := null;
      l_mo.UNMEASUREDTYPECDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEDDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEEDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEFDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEGDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEHDESCRIPTION := null;
      l_mo.PIPESIZE := null;

      IF t_prop(i).WADJCHARGEADJTARIFFCODE IS NOT NULL THEN
         l_mo.SERVICECOMPONENTTYPE := 'WCA';
         l_mo.TARIFFCODE_PK := t_prop(i).WADJCHARGEADJTARIFFCODE;
         FN_ADD_DATA;
      END IF;

      -- Metered Foul Sewage (MS)
      IF (    t_prop(i).METEREDFSTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'MS';
         l_no_row_ms := l_no_row_ms + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).METEREDFSTARIFFCODE;  
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).MFSSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).MFSSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).MFSSPECIALAGREEMENTREF;
         l_mo.METEREDFSMAXDAILYDEMAND := NULL;               --- **** ??
         l_mo.METEREDFSDAILYRESVDCAPACITY := NULL;           --- **** ??
         FN_ADD_DATA;
      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := NULL;
      l_mo.METEREDFSMAXDAILYDEMAND := NULL;               --- **** ??
      l_mo.METEREDFSDAILYRESVDCAPACITY := NULL;           --- **** ??

      -- Assessed Sewage(AS)

      IF (    t_prop(i).ASASSESSEDTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'AS';
         l_no_row_as := l_no_row_as + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).ASASSESSEDTARIFFCODE;    
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).ASSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).ASSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).ASSPECIALAGREEMENTREF;
         l_mo.ASSESSEDDVOLUMETRICRATE	:= t_prop(i).ASASSESSEDDVOLUMETRICRATE;
         l_mo.ASSESSEDCHARGEMETERSIZE := t_prop(i).ASASSESSEDCHARGEMETERSIZE;
         l_mo.ASSESSEDTARIFBAND := t_prop(i).ASASSESSEDTARIFBAND;

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
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'ASSESSEDDVOLUMETRICRATE has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
               l_no_row_exp := l_no_row_exp + 1;
               l_rec_written := FALSE;
            END IF;
         END IF;

         IF l_rec_written THEN
            FN_ADD_DATA;
         END IF;

      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := null;
      l_mo.ASSESSEDDVOLUMETRICRATE	:= null;
      l_mo.ASSESSEDCHARGEMETERSIZE := null;
      l_mo.ASSESSEDTARIFBAND := null;

      -- Unmeasured Sewage(US)
      IF (    t_prop(i).USUNMEASUREDTARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'US';
         l_no_row_us := l_no_row_us + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).USUNMEASUREDTARIFFCODE; 
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).USSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).USSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).USSPECIALAGREEMENTREF;                      
         l_mo.UNMEASUREDTYPEACOUNT	:= t_prop(i).USUNMEASUREDTYPEACOUNT;
         l_mo.UNMEASUREDTYPEBCOUNT := t_prop(i).USUNMEASUREDTYPEBCOUNT;
         l_mo.UNMEASUREDTYPECCOUNT := t_prop(i).USUNMEASUREDTYPECCOUNT;
         l_mo.UNMEASUREDTYPEDCOUNT	:= t_prop(i).USUNMEASUREDTYPEDCOUNT;
         l_mo.UNMEASUREDTYPEECOUNT := t_prop(i).USUNMEASUREDTYPEECOUNT;
         l_mo.UNMEASUREDTYPEFCOUNT := t_prop(i).USUNMEASUREDTYPEFCOUNT;
         l_mo.UNMEASUREDTYPEGCOUNT := t_prop(i).USUNMEASUREDTYPEGCOUNT;
         l_mo.UNMEASUREDTYPEHCOUNT := t_prop(i).USUNMEASUREDTYPEHCOUNT;
         l_mo.UNMEASUREDTYPEADESCRIPTION := t_prop(i).USUNMEASUREDTYPEADESCRIPTION;
         l_mo.UNMEASUREDTYPEBDESCRIPTION := t_prop(i).USUNMEASUREDTYPEBDESCRIPTION;
         l_mo.UNMEASUREDTYPECDESCRIPTION := t_prop(i).USUNMEASUREDTYPECDESCRIPTION;
         l_mo.UNMEASUREDTYPEDDESCRIPTION := t_prop(i).USUNMEASUREDTYPEDDESCRIPTION;
         l_mo.UNMEASUREDTYPEEDESCRIPTION := t_prop(i).USUNMEASUREDTYPEEDESCRIPTION;
         l_mo.UNMEASUREDTYPEFDESCRIPTION := t_prop(i).USUNMEASUREDTYPEFDESCRIPTION;
         l_mo.UNMEASUREDTYPEGDESCRIPTION := t_prop(i).USUNMEASUREDTYPEGDESCRIPTION;
         l_mo.UNMEASUREDTYPEHDESCRIPTION := t_prop(i).USUNMEASUREDTYPEHDESCRIPTION;
         l_mo.PIPESIZE := t_prop(i).USPIPESIZE;

         IF nvl(l_mo.PIPESIZE,1) <> 0 THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'PIPESIZE must be 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
         ELSE 
            FN_ADD_DATA;
         END IF;

      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := null;                    
      l_mo.UNMEASUREDTYPEACOUNT	:= null;
      l_mo.UNMEASUREDTYPEBCOUNT := null;
      l_mo.UNMEASUREDTYPECCOUNT := null;
      l_mo.UNMEASUREDTYPEDCOUNT	:= null;
      l_mo.UNMEASUREDTYPEECOUNT := null;
      l_mo.UNMEASUREDTYPEFCOUNT := null;
      l_mo.UNMEASUREDTYPEGCOUNT := null;
      l_mo.UNMEASUREDTYPEHCOUNT := null;
      l_mo.UNMEASUREDTYPEADESCRIPTION := null;
      l_mo.UNMEASUREDTYPEBDESCRIPTION := null;
      l_mo.UNMEASUREDTYPECDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEDDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEEDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEFDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEGDESCRIPTION := null;
      l_mo.UNMEASUREDTYPEHDESCRIPTION := null;
      l_mo.PIPESIZE := null;

      IF t_prop(i).SADJCHARGEADJTARIFFCODE IS NOT NULL THEN
         l_mo.SERVICECOMPONENTTYPE := 'SCA';
         l_mo.TARIFFCODE_PK := t_prop(i).SADJCHARGEADJTARIFFCODE;         
         FN_ADD_DATA;
      END IF;

      -- Surface Water (SW)

     IF (    t_prop(i).SRFCWATERTARRIFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'SW';
         l_no_row_sw := l_no_row_sw + 1;
         l_mo.TARIFFCODE_PK := t_prop(i).SRFCWATERTARRIFCODE;   
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).SWSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).SWSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).SWSPECIALAGREEMENTREF; 
         l_mo.SRFCWATERAREADRAINED := t_prop(i).SRFCWATERAREADRAINED;
         l_mo.SRFCWATERCOMMUNITYCONFLAG := t_prop(i).SRFCWATERCOMMUNITYCONFLAG;

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

--         IF nvl(l_sw.SWRVPOUNDAGE,0)  > 0 THEN
--            IF l_mo.SRFCWATERAREADRAINED <> 0 THEN
--               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'SRFCWATERAREADRAINED MUST BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
--               l_no_row_exp := l_no_row_exp + 1;
--               l_rec_written := FALSE;
--            END IF;
--         END IF;
--
--         IF nvl(l_sw.SWRVPOUNDAGE,0)  = 0 THEN
--            IF l_mo.SRFCWATERAREADRAINED = 0 THEN
--               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'SRFCWATERAREADRAINED CANNOT BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
--               l_no_row_exp := l_no_row_exp + 1;
--               l_rec_written := FALSE;
--            END IF;
--         END IF;

         IF l_rec_written THEN
            FN_ADD_DATA;
         END IF;
         
      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := null;
      l_mo.SPECIALAGREEMENTREF := null;
      l_mo.SRFCWATERAREADRAINED := 0;
      l_mo.SRFCWATERCOMMUNITYCONFLAG := 0;
      
      -- Highway Drainage (HD)
      IF (    t_prop(i).HWAYDRAINAGETARIFFCODE IS NOT NULL 
          AND l_rec_written                            )
      THEN
         l_mo.SERVICECOMPONENTTYPE := 'HD';
         l_mo.TARIFFCODE_PK := t_prop(i).HWAYDRAINAGETARIFFCODE;      
         l_mo.SPECIALAGREEMENTFLAG := t_prop(i).HDSPECIALAGREEMENTFLAG;
         l_mo.SPECIALAGREEMENTFACTOR := t_prop(i).HDSPECIALAGREEMENTFACTOR;
         l_mo.SPECIALAGREEMENTREF := t_prop(i).HDSPECIALAGREEMENTREF;    
         l_mo.HWAYSURFACEAREA	:= t_prop(i).HWAYSURFACEAREA;
         l_mo.HWAYCOMMUNITYCONFLAG := t_prop(i).HWAYCOMMUNITYCONFLAG;
         FN_ADD_DATA;
      END IF;
      l_mo.SPECIALAGREEMENTFLAG := null;
      l_mo.SPECIALAGREEMENTFACTOR := NULL;
      l_mo.SPECIALAGREEMENTREF := null;
      l_mo.HWAYSURFACEAREA	:= NULL;
      l_mo.HWAYCOMMUNITYCONFLAG := 0;
       
--         -- State of service component
--
--         l_progress := 'SELECT TVP056SERVPROV';
--         SELECT CASE  WHEN ST_SERV_PROV IN ('A', 'C', 'G')
--                 THEN 1
--                 ELSE 0
--                END AS D2076_ACTIVE,
--                DT_START
--         INTO   l_mo.SERVICECOMPONENTENABLED,
--                l_mo.EFFECTIVEFROMDATE
--         FROM   CIS.TVP056SERVPROV
--         WHERE  STWPROPERTYNUMBER  = t_prop(i).STWPROPERTYNUMBER_PK
--         AND    SPID_PK = t_prop(i).SPID_PK;
--
--         SELECT MAX(NVL(SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')))
--         INTO  l_mo.EFFECTIVEFROMDATE
--         FROM MO_SUPPLY_POINT
--         WHERE SPID_PK = l_mo.SPID_PK;

        l_prev_prp := t_prop(i).STWPROPERTYNUMBER_PK;
        l_prev_tariff := l_mo.TARIFFCODE_PK;

    END LOOP;

    IF t_prop.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE cur_prop;

  -- write counts
--  l_progress := 'Writing Counts';
--
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1030, l_no_row_read,    'Read in to transform');
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1040, l_no_row_dropped, 'Dropped during Transform');
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1050, l_no_row_insert,  'Written to Table ');
--  
--  SELECT COUNT(DISTINCT spid_pk)
--  INTO   l_count_spids
--  FROM   MO_SERVICE_COMPONENT;
--  
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1051, l_count_spids,  'Number of SPIDs in Table ');
--
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 700, l_no_row_mpw,  'Distinct Service Component Type MPW');
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 750, l_no_row_as,  'Distinct Service Component Type AS');
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 720, l_no_row_aw,  'Distinct Service Component Type AW');  
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 740, l_no_row_ms,  'Distinct Service Component Type MS');    
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 770, l_no_row_sw,  'Distinct Service Component Type SW');    
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 760, l_no_row_us,  'Distinct Service Component Type US');    
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 730, l_no_row_uw,  'Distinct Service Component Type UW');    

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