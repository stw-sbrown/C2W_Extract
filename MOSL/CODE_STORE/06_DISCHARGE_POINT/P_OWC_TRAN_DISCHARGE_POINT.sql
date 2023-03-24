CREATE OR REPLACE PROCEDURE P_OWC_TRAN_DISCHARGE_POINT(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                       no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                       return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_OWC_TRAN_DISCHARGE_POINT.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_DISCHARGE_POINT from OWC supplied data 
--               - OWC_DISCHARGE_POINT.
--
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_DISCHARGE_POINT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE; 
  l_prev_floc                   MO_ELIGIBLE_PREMISES.SAPFLOCNUMBER%TYPE := 0; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_DISCHARGE_POINT%ROWTYPE; 
  l_spid                        LU_SPID_RANGE%ROWTYPE;   
  l_adr                         MO_ADDRESS%ROWTYPE;  
  l_adr_pr                      MO_PROPERTY_ADDRESS%ROWTYPE := NULL;  
  l_mot                         MO_TARIFF%ROWTYPE;  
  l_sp                          MO_SUPPLY_POINT%ROWTYPE;
  l_vol                         LU_DISCHARGE_VOL_LIMITS%ROWTYPE;
  l_lu                          LU_SPID_RANGE%ROWTYPE;
  l_mtv                         MO_TARIFF_VERSION%ROWTYPE;  
  l_band                        MO_TE_BAND_CHARGE%ROWTYPE;  
  l_tet                         MO_TARIFF_TYPE_TE%ROWTYPE;
  l_robt                        MO_TE_BLOCK_ROBT%ROWTYPE;
  l_bobt                        MO_TE_BLOCK_BOBT%ROWTYPE;  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_no_row_dropped_cd_adr       MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_insert_adr           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_no_row_dropped_prop         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_insert_adrprop       MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_rec_written                 BOOLEAN;

CURSOR cur_prop (p_property_start   MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_property_end     MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE)                 
    IS 
    SELECT  dp.SPID_PK,
            dp.DPID_PK,
            dp.TARRIFCODE,
            dp.TARRIFBAND,
            dp.TREFODCHEMOXYGENDEMAND,
            dp.TREFODCHEMSUSPSOLDEMAND,
            dp.TREFODCHEMAMONIANITROGENDEMAND,
            dp.TREFODCHEMCOMPXDEMAND,
            dp.TREFODCHEMCOMPYDEMAND,
            dp.TREFODCHEMCOMPZDEMAND,
            dp.SEWERAGEVOLUMEADJMENTHOD,
            dp.RECEPTIONTREATMENTINDICATOR,
            dp.PRIMARYTREATMENTINDICATOR,
            dp.MARINETREATMENTINDICATOR,
            dp.BIOLOGICALTREATMENTINDICATOR,
            dp.SLUDGETREATMENTINDICATOR,
            dp.AMMONIATREATMENTINDICATOR,
            dp.TEFXTREATMENTINDICATOR,
            dp.TEFYTREATMENTINDICATOR,
            dp.TEFZTREATMENTINDICATOR,
            dp.TEFAVAILABILITYDATAX,
            dp.TEFAVAILABILITYDATAY,
            dp.TEFAVAILABILITYDATAZ,
            dp.CHARGEABLEDAILYVOL,
            dp.CHEMICALOXYGENDEMAND,
            dp.SUSPENDEDSOLIDSLOAD,
            dp.AMMONIANITROCAL,
            dp.FIXEDALLOWANCE,
            dp.PERCENTAGEALLOWANCE,
            dp.DOMMESTICALLOWANCE,
            dp.SEASONALFACTOR,
            dp.DPIDSPECIALAGREEMENTINPLACE,
            dp.DPIDSPECIALAGREEMENTFACTOR,
            dp.DPIDSPECIALAGREEMENTREFERENCE,
            dp.FREETEXTDESCRIPTOR,
            dp.SECONDADDRESSABLEOBJ,
            dp.PRIMARYADDRESSABLEOBJ,
            dp.ADDRESSLINE01,
            dp.ADDRESSLINE02,
            dp.ADDRESSLINE03,
            dp.ADDRESSLINE04,
            dp.ADDRESSLINE05,
            dp.POSTCODE,
            dp.PAFADDRESSKEY,
            dp.VALIDTETARIFFCODE,
            dp.TARIFFBANDCOUNT,
            dp.AMMONIACALNITROGEN,
            dp.XCOMP,
            dp.YCOMP,
            dp.ZCOMP,
            dp.STWIWCS,
            dp.SAPFLOCNUMBER,
            dp.STWCONSENTNUMBER,
            dp.OWC,
            pr.STWPROPERTYNUMBER_PK
    FROM    RECEPTION.SAP_DISCHARGE_POINT dp 
            LEFT JOIN MO_ELIGIBLE_PREMISES pr ON pr.CORESPID_PK = substr(dp.SPID_PK,1,10) 
    WHERE   pr.STWPROPERTYNUMBER_PK BETWEEN p_property_start AND p_property_end
    ORDER BY pr.STWPROPERTYNUMBER_PK; 

TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
BEGIN
 
   -- initialise variables 
   
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
    
      l_err.TXT_KEY := substr(t_prop(i).STWPROPERTYNUMBER_PK || ',' || t_prop(i).SPID_PK || ',' || t_prop(i).DPID_PK,1,30);
          
      l_mo := NULL;
      l_rec_written := TRUE;

      -- keep count of distinct property
      l_no_row_read := l_no_row_read + 1;

      l_mo.STWPROPERTYNUMBER_PK := t_prop(i).STWPROPERTYNUMBER_PK;      
      l_mo.NO_ACCOUNT := null;      
    
      l_mo.DPID_PK := t_prop(i).DPID_PK;  
      l_mo.NO_SAMPLE_POINT := NULL;         
      l_mo.CONSENT_NO := t_prop(i).STWCONSENTNUMBER;       
      l_mo.SPID_PK := t_prop(i).SPID_PK;
      l_mo.NO_IWCS := t_prop(i).STWIWCS;

      -- Set tariff details

      l_mo.TARRIFCODE := t_prop(i).TARRIFCODE;
      
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

      l_progress := 'SELECT MO_TARIFF_VERSION/MO_TARIFF_TYPE_TE';
      BEGIN      
         SELECT mtv.TARIFF_VERSION_PK,
                tet.TARIFF_TYPE_PK,
                mot.SERVICECOMPONENTTYPE,
                tet.TECHARGECOMPOS,            -- Chemical Oxygen Demand D7566
                tet.TECHARGECOMPSS,            -- suspended solids D7567 
                tet.TECHARGECOMPAS,            -- Base value of Ammoniacal Nitrogen D7568
                tet.TECHARGECOMPSO,            -- Sludge Treatment D7564
                tet.TECHARGECOMPAA	           -- Ammonia capacity charging component D7558
         INTO   l_mtv.TARIFF_VERSION_PK,
                l_tet.TARIFF_TYPE_PK,
                l_mot.SERVICECOMPONENTTYPE,
                l_tet.TECHARGECOMPOS,
                l_tet.TECHARGECOMPSS,
                l_tet.TECHARGECOMPAS,
                l_tet.TECHARGECOMPSO,
                l_tet.TECHARGECOMPAA
         FROM   MO_TARIFF_VERSION   mtv,
                MO_TARIFF_TYPE_TE   tet,
                MO_TARIFF           mot
         WHERE  mot.TARIFFCODE_PK     = l_mo.TARRIFCODE
         AND    mtv.TARIFFCODE_PK     = mot.TARIFFCODE_PK
         AND    mtv.TARIFFVERSION     = (SELECT MAX(TARIFFVERSION) 
                                         FROM   MO_TARIFF_VERSION
                                         WHERE  TARIFFCODE_PK = mtv.TARIFFCODE_PK)
         AND    tet.TARIFF_VERSION_PK = mtv.TARIFF_VERSION_PK;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_mtv.TARIFF_VERSION_PK := 0;
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;
  
      l_mo.SERVICECOMPTYPE := l_mot.SERVICECOMPONENTTYPE;
      
      -- Set Effective Dates
       
      SELECT MAX(NVL(SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')))
      INTO  l_sp.SUPPLYPOINTEFFECTIVEFROMDATE
      FROM  MO_SUPPLY_POINT
      WHERE SPID_PK = l_mo.SPID_PK;

      l_mo.SCEFFECTIVEFROMDATE := l_sp.SUPPLYPOINTEFFECTIVEFROMDATE;
      l_mo.DPEFFECTFROMDATE := l_sp.SUPPLYPOINTEFFECTIVEFROMDATE;
      l_mo.DPEFFECTTODATE := null;      

      l_mo.EFFECTFROMDATE := l_sp.SUPPLYPOINTEFFECTIVEFROMDATE;      
      l_mo.EFFECTTODATE := NULL;  
        
      -- Status of service component
      l_mo.DISCHARGEPOINTERASEFLAG := null;
      l_mo.WHOLESALERID := 'SEVERN-W';   

      -- Update Trade Effluent Availability 
  
      l_mo.CHEMICALOXYGENDEMAND := t_prop(i).CHEMICALOXYGENDEMAND;
      l_mo.SUSPENDEDSOLIDSLOAD := t_prop(i).SUSPENDEDSOLIDSLOAD;
      l_mo.AMMONIANITROCAL := t_prop(i).AMMONIANITROCAL;
      l_mo.CHARGEABLEDAILYVOL := t_prop(i).CHARGEABLEDAILYVOL;  
      
      -- Update Seasonal Factor

      l_mo.SEASONALFACTOR := t_prop(i).SEASONALFACTOR;   
      l_mo.DOMMESTICALLOWANCE := t_prop(i).DOMMESTICALLOWANCE;
      l_mo.FIXEDALLOWANCE := t_prop(i).FIXEDALLOWANCE;
      l_mo.PERCENTAGEALLOWANCE := t_prop(i).PERCENTAGEALLOWANCE;
      l_mo.SEASONALFACTOR := t_prop(i).SEASONALFACTOR;
      l_mo.SEWERAGEVOLUMEADJMENTHOD := t_prop(i).SEWERAGEVOLUMEADJMENTHOD;
      l_mo.RECEPTIONTREATMENTINDICATOR := t_prop(i).RECEPTIONTREATMENTINDICATOR;

      -- Check Reception Indicator
      
      l_progress := 'SELECT MO_TE_BLOCK_ROBT';  
      IF l_mo.RECEPTIONTREATMENTINDICATOR = 1 THEN
          BEGIN
            SELECT CHARGE
            INTO   l_robt.CHARGE
            FROM   MO_TE_BLOCK_ROBT
            WHERE  TARIFF_TYPE_PK = l_tet.TARIFF_TYPE_PK
            AND    ROWNUM = 1;
          EXCEPTION
          WHEN NO_DATA_FOUND THEN
               l_rec_written := FALSE;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'RECEPTIONTREATMENTINDICATOR MUST BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
               l_no_row_exp := l_no_row_exp + 1;
          END;
      END IF;

      l_mo.PRIMARYTREATMENTINDICATOR := t_prop(i).PRIMARYTREATMENTINDICATOR;

      -- Set Primary Treatment Indicator

      l_progress := 'SELECT MO_TE_BLOCK_BOBT'; 
      l_bobt.CHARGE := 0;
     
      BEGIN
         SELECT CHARGE
         INTO   l_bobt.CHARGE
         FROM   MO_TE_BLOCK_BOBT
         WHERE  TARIFF_TYPE_PK = l_tet.TARIFF_TYPE_PK
         AND    ROWNUM = 1;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           IF l_mo.PRIMARYTREATMENTINDICATOR = 1 THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'PRIMARYTREATMENTINDICATOR MUST BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
              l_no_row_exp := l_no_row_exp + 1;
           END IF;
      END;
      
      l_mo.MARINETREATMENTINDICATOR := t_prop(i).MARINETREATMENTINDICATOR;
      l_mo.BIOLOGICALTREATMENTINDICATOR := t_prop(i).BIOLOGICALTREATMENTINDICATOR;

      -- Check Biological Treatment Indicator
      
      IF nvl(l_bobt.CHARGE,0) > 0 THEN
         IF l_mo.BIOLOGICALTREATMENTINDICATOR <>  1 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'BIOLOGICALTREATMENTINDICATOR MUST BE 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
      ELSE
         IF l_mo.BIOLOGICALTREATMENTINDICATOR <> 0 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'BIOLOGICALTREATMENTINDICATOR MUST BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;         
      END IF;

      l_mo.SLUDGETREATMENTINDICATOR := t_prop(i).SLUDGETREATMENTINDICATOR;

      -- Set Sludge Treatment Indicator
      
      IF nvl(l_tet.TECHARGECOMPSO,0) > 0 THEN
         IF l_mo.SLUDGETREATMENTINDICATOR <> 1 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'SLUDGETREATMENTINDICATOR MUST BE 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
      ELSE
         IF l_mo.SLUDGETREATMENTINDICATOR <> 0 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'SLUDGETREATMENTINDICATOR MUST BE 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;         
         END IF;
      END IF;

      
      l_mo.TEFXTREATMENTINDICATOR := t_prop(i).TEFXTREATMENTINDICATOR;
      l_mo.TEFYTREATMENTINDICATOR := t_prop(i).TEFYTREATMENTINDICATOR;
      l_mo.TEFZTREATMENTINDICATOR := t_prop(i).TEFZTREATMENTINDICATOR;
      l_mo.TARRIFBAND := t_prop(i).TARRIFBAND;
      l_mo.TREFODCHEMOXYGENDEMAND := t_prop(i).TREFODCHEMOXYGENDEMAND;

      -- Check Oxygen demand
      
      IF l_tet.TECHARGECOMPOS > 0 THEN 
         IF NVL(l_mo.TREFODCHEMOXYGENDEMAND,0) = 0 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No agreed limit or average strength for Oxygen demand',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
      END IF;

      l_mo.TREFODCHEMSUSPSOLDEMAND := t_prop(i).TREFODCHEMSUSPSOLDEMAND;

      -- Check solids
      
      IF l_tet.TECHARGECOMPSS > 0 THEN 
         IF NVL(l_mo.TREFODCHEMSUSPSOLDEMAND,0) = 0 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No agreed limit or average strength for Suspended Solids',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
      END IF;
      
      l_mo.TREFODCHEMAMONIANITROGENDEMAND := t_prop(i).TREFODCHEMAMONIANITROGENDEMAND;
  
      -- set Ammoniacal Nitrogen demand
     
      l_mo.TREFODCHEMAMONIANITROGENDEMAND := NULL ;
     
      IF (    l_mo.AMMONIATREATMENTINDICATOR = 1 
          AND l_mo.TREFODCHEMAMONIANITROGENDEMAND IS NULL)
      THEN
         l_rec_written := FALSE;
         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No Ammonia Nitrogen demand',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
         l_no_row_exp := l_no_row_exp + 1;
      END IF;

      l_mo.AMMONIATREATMENTINDICATOR := t_prop(i).AMMONIATREATMENTINDICATOR;     


      -- check Sewerage volumen adjustment method

       IF (   l_mo.SEWERAGEVOLUMEADJMENTHOD = 'NONE'
           OR l_mo.SEWERAGEVOLUMEADJMENTHOD = 'SUBTRACT')
       THEN   
          IF l_mo.DOMMESTICALLOWANCE is not null THEN      
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'DOMMESTICALLOWANCE should be null',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
             l_no_row_exp := l_no_row_exp + 1;
             l_rec_written := FALSE;
          END IF;
       ELSE
           IF nvl(l_mo.DOMMESTICALLOWANCE,0) = 0 THEN      
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'DOMMESTICALLOWANCE should not be null',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
              l_no_row_exp := l_no_row_exp + 1;
              l_rec_written := FALSE;
           END IF;
       END IF;
   
      -- Set Trade Effluent components
        
      l_mo.TREFODCHEMCOMPXDEMAND := t_prop(i).TREFODCHEMCOMPXDEMAND; 
      l_mo.TREFODCHEMCOMPYDEMAND := t_prop(i).TREFODCHEMCOMPYDEMAND; 
      l_mo.TREFODCHEMCOMPZDEMAND := t_prop(i).TREFODCHEMCOMPZDEMAND; 
      l_mo.DPIDSPECIALAGREEMENTINPLACE := t_prop(i).DPIDSPECIALAGREEMENTINPLACE;
      l_mo.DPIDSPECIALAGREEMENTFACTOR := t_prop(i).DPIDSPECIALAGREEMENTFACTOR;
      l_mo.DPIDSPECIALAGREEMENTREFERENCE := t_prop(i).DPIDSPECIALAGREEMENTREFERENCE;

  
    -- Set up volume limits
  
      l_mo.VOLUME_LIMIT := NULL;
      l_mo.DPID_TYPE := null;
      
      -- Get service component type for tariff
    
      l_progress := 'SELECT MO_TARIFF ';
      BEGIN
         SELECT SERVICECOMPONENTTYPE
         INTO   l_mot.SERVICECOMPONENTTYPE
         FROM   MO_TARIFF
         WHERE  TARIFFCODE_PK = l_mo.TARRIFCODE;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
            l_mot.SERVICECOMPONENTTYPE := null;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARRIFCODE,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
      END;
      l_mo.SERVICECOMPTYPE := l_mot.SERVICECOMPONENTTYPE;
    
     l_progress := 'INSERT MO_DISCHARGE_POINT ';           
    
     IF l_rec_written THEN
        BEGIN 
          INSERT INTO MO_DISCHARGE_POINT
          (DPID_PK, SPID_PK, SERVICECOMPTYPE, SCEFFECTIVEFROMDATE, DPEFFECTFROMDATE, DPEFFECTTODATE, DISCHARGEPOINTERASEFLAG, EFFECTFROMDATE, EFFECTTODATE, WHOLESALERID,
           TARRIFCODE, CHARGEABLEDAILYVOL, AMMONIANITROCAL, CHEMICALOXYGENDEMAND, SUSPENDEDSOLIDSLOAD, DOMMESTICALLOWANCE, SEASONALFACTOR, PERCENTAGEALLOWANCE, FIXEDALLOWANCE,
           RECEPTIONTREATMENTINDICATOR, PRIMARYTREATMENTINDICATOR, MARINETREATMENTINDICATOR, BIOLOGICALTREATMENTINDICATOR, SLUDGETREATMENTINDICATOR, AMMONIATREATMENTINDICATOR,
           TEFXTREATMENTINDICATOR, TEFYTREATMENTINDICATOR, TEFZTREATMENTINDICATOR, TEFAVAILABILITYDATAX, TEFAVAILABILITYDATAY, TEFAVAILABILITYDATAZ, TARRIFBAND, 
           SEWERAGEVOLUMEADJMENTHOD, SECONDADDRESSABLEOBJ, PRIMARYADDRESSABLEOBJ, TREFODCHEMOXYGENDEMAND, TREFODCHEMSUSPSOLDEMAND, TREFODCHEMAMONIANITROGENDEMAND, 
           TREFODCHEMCOMPXDEMAND, TREFODCHEMCOMPYDEMAND, TREFODCHEMCOMPZDEMAND, DPIDSPECIALAGREEMENTINPLACE, DPIDSPECIALAGREEMENTFACTOR, DPIDSPECIALAGREEMENTREFERENCE,
           NO_IWCS, NO_SAMPLE_POINT, NO_ACCOUNT, CONSENT_NO, STWPROPERTYNUMBER_PK, VOLUME_LIMIT, DPID_TYPE)
           VALUES
           (l_mo.DPID_PK, l_mo.SPID_PK, l_mo.SERVICECOMPTYPE, l_mo.SCEFFECTIVEFROMDATE, l_mo.DPEFFECTFROMDATE, l_mo.DPEFFECTTODATE, l_mo.DISCHARGEPOINTERASEFLAG, l_mo.EFFECTFROMDATE, l_mo.EFFECTTODATE, l_mo.WHOLESALERID,
            l_mo.TARRIFCODE, l_mo.CHARGEABLEDAILYVOL, l_mo.AMMONIANITROCAL, l_mo.CHEMICALOXYGENDEMAND, l_mo.SUSPENDEDSOLIDSLOAD, l_mo.DOMMESTICALLOWANCE, l_mo.SEASONALFACTOR, l_mo.PERCENTAGEALLOWANCE, l_mo.FIXEDALLOWANCE,
            l_mo.RECEPTIONTREATMENTINDICATOR, l_mo.PRIMARYTREATMENTINDICATOR, l_mo.MARINETREATMENTINDICATOR, l_mo.BIOLOGICALTREATMENTINDICATOR, l_mo.SLUDGETREATMENTINDICATOR, l_mo.AMMONIATREATMENTINDICATOR,
            l_mo.TEFXTREATMENTINDICATOR, l_mo.TEFYTREATMENTINDICATOR, l_mo.TEFZTREATMENTINDICATOR, l_mo.TEFAVAILABILITYDATAX, l_mo.TEFAVAILABILITYDATAY, l_mo.TEFAVAILABILITYDATAZ, l_mo.TARRIFBAND,
            l_mo.SEWERAGEVOLUMEADJMENTHOD, l_mo.SECONDADDRESSABLEOBJ, l_mo.PRIMARYADDRESSABLEOBJ, l_mo.TREFODCHEMOXYGENDEMAND, l_mo.TREFODCHEMSUSPSOLDEMAND, l_mo.TREFODCHEMAMONIANITROGENDEMAND, 
            l_mo.TREFODCHEMCOMPXDEMAND, l_mo.TREFODCHEMCOMPYDEMAND, l_mo.TREFODCHEMCOMPZDEMAND, l_mo.DPIDSPECIALAGREEMENTINPLACE, l_mo.DPIDSPECIALAGREEMENTFACTOR, l_mo.DPIDSPECIALAGREEMENTREFERENCE,
            l_mo.NO_IWCS, l_mo.NO_SAMPLE_POINT, l_mo.NO_ACCOUNT, l_mo.CONSENT_NO, l_mo.STWPROPERTYNUMBER_PK, l_mo.VOLUME_LIMIT, l_mo.DPID_TYPE);            
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK,1,100));      -- LSmith V0.03
             l_no_row_err := l_no_row_err + 1;
        END;
     ELSE
        l_no_row_dropped := l_no_row_dropped + 1;
     END IF;  

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
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1060, l_no_row_read,    'Read in to transform');
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1070, l_no_row_dropped, 'Dropped during Transform');   
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1080, l_no_row_insert,  'Written to Table ');    

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
END P_OWC_TRAN_DISCHARGE_POINT;
/
show error;

exit;