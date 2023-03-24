CREATE OR REPLACE PROCEDURE P_FIN_TRAN_METER(no_batch          IN MIG_BATCHSTATUS.no_batch%TYPE,
                                             no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                             return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_FIN_TRAN_METER.sql
--
-- Subversion $Revision: 5379 $
--
-- CREATED        : 04/08/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER from SAP and OWC supplied data 
--               - OWC_METER and SAP_METER.
--
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      04/08/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_FIN_TRAN_METER';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE; 
  l_prev_floc                   MO_ELIGIBLE_PREMISES.SAPFLOCNUMBER%TYPE := 0; 
  l_dum_meterref                MO_METER.METERREF%TYPE := 0; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_METER%ROWTYPE; 
  l_spid                        LU_SPID_RANGE%ROWTYPE;   
  l_adr                         MO_ADDRESS%ROWTYPE;  
  l_dp                          MO_DISCHARGE_POINT%ROWTYPE;  
  l_adr_mt                      MO_METER_ADDRESS%ROWTYPE := NULL;  
  l_mot                         MO_TARIFF%ROWTYPE;  
  l_sp                          MO_SUPPLY_POINT%ROWTYPE;
  l_vol                         LU_DISCHARGE_VOL_LIMITS%ROWTYPE;
  l_mt                          LU_METER_MANUFACTURER%ROWTYPE;
  l_lu                          LU_SPID_RANGE%ROWTYPE;  
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
  l_no_cd_adr                   NUMBER(9) := 0;
  l_sc_count                    NUMBER;
  l_gis_code                    VARCHAR2(60);
  l_no_equipment                NUMBER := 0;
  l_marketable_meter_cnt        NUMBER := 0;
  l_marketable_new_meter_cnt    NUMBER := 0;
  
CURSOR cur_prop (p_property_start   MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_property_end     MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE)                 
    IS 

    SELECT  mt.MANUFACTURERSERIALNUM_PK,
            mt.MANUFACTURER_PK,
            mt.NUMBEROFDIGITS,
            mt.MEASUREUNITATMETER,
            mt.MEASUREUNITFREEDESCRIPTOR,
            mt.PHYSICALMETERSIZE,
            mt.METERREADFREQUENCY,
            mt.INITIALMETERREADDATE,
            mt.RETURNTOSEWER,
            mt.WATERCHARGEMETERSIZE,
            mt.SEWCHARGEABLEMETERSIZE,
            mt.DATALOGGERWHOLESALER,
            mt.DATALOGGERNONWHOLESALER,
            mt.GPSX,
            mt.GPSY,
            mt.METERLOCFREEDESCRIPTOR,
            mt.METEROUTREADERGPSX,
            mt.METEROUTREADERGPSY,
            mt.OUTREADERLOCFREEDES,
            mt.METEROUTREADERLOCCODE,
            mt.METERTREATMENT,
            mt.METERLOCATIONCODE,
            mt.COMBIMETERFLAG,
            mt.YEARLYVOLESTIMATE,
            mt.REMOTEREADFLAG,
            mt.REMOTEREADTYPE,
            mt.OUTREADERID,
            mt.OUTREADERPROTOCOL,
            mt.LOCATIONFREETEXTDESCRIPTOR,
            mt.SECONDADDRESABLEOBJECT,
            mt.PRIMARYADDRESSABLEOBJECT,
            mt.ADDRESSLINE01,
            mt.ADDRESSLINE02,
            mt.ADDRESSLINE03,
            mt.ADDRESSLINE04,
            mt.ADDRESSLINE05,
            mt.POSTCODE,
            mt.PAFADDRESSKEY,
            mt.SPID,
            mt.NONMARKETMETERFLAG,
            mt.SAPEQUIPMENT,
            mt.SAPFLOCNUMBER,
            mt.OWC,
            pr.STWPROPERTYNUMBER_PK
    FROM    RECEPTION.SAP_METER mt 
            LEFT JOIN MO_ELIGIBLE_PREMISES pr ON pr.CORESPID_PK = substr(mt.SPID,1,10) 
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
     
   l_progress := 'processing ';

   -- any errors set return code and exit out
   
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;
      
  -- start processing all records for range supplied
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'SELECT  MO_ADDRESS';

  -- get max address key
  
  BEGIN 
     SELECT MAX(ADDRESS_PK) 
     INTO   l_adr.ADDRESS_PK
     FROM   MO_ADDRESS;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
       l_adr.ADDRESS_PK := 0;
  END;
  l_no_cd_adr := l_adr.ADDRESS_PK; 
            
  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := substr(t_prop(i).STWPROPERTYNUMBER_PK || ',' || t_prop(i).SPID || ',' || t_prop(i).MANUFACTURERSERIALNUM_PK,1,30);


      l_progress := 'Set up values';          
      l_mo := NULL;
      l_rec_written := TRUE;

      -- keep count of distinct property
      l_no_row_read := l_no_row_read + 1;

      l_mo.COMBIMETERFLAG := t_prop(i).COMBIMETERFLAG;     
      l_mo.DATALOGGERNONWHOLESALER := t_prop(i).DATALOGGERNONWHOLESALER;
      l_mo.DATALOGGERWHOLESALER := t_prop(i).DATALOGGERWHOLESALER; 
      l_mo.FREEDESCRIPTOR := null;                ---- fix  ********  
      l_mo.GPSX := t_prop(i).GPSX;  
      l_mo.GPSY := t_prop(i).GPSY;        
      l_mo.MANUFACTURER_PK := t_prop(i).MANUFACTURER_PK;          
      l_mo.MANUFACTURERSERIALNUM_PK := t_prop(i).MANUFACTURERSERIALNUM_PK;   
      l_mo.MDVOL := NULL;    
      l_mo.DPID_PK := null;
      l_mo.MEASUREUNITATMETER := t_prop(i).MEASUREUNITATMETER;    
      l_mo.MEASUREUNITFREEDESCRIPTOR := t_prop(i).MEASUREUNITFREEDESCRIPTOR;    
 
      l_mo.METERADDITIONREASON := null;      
      l_mo.METERLOCATIONCODE := t_prop(i).METERLOCATIONCODE;    
      l_mo.METERLOCFREEDESCRIPTOR := t_prop(i).METERLOCFREEDESCRIPTOR;    
      l_mo.METERNETWORKASSOCIATION := 0;    
      l_mo.METEROUTREADERGPSX := t_prop(i).METEROUTREADERGPSX;    
   
      l_mo.METEROUTREADERGPSY := t_prop(i).METEROUTREADERGPSY;    
      l_mo.METEROUTREADERLOCCODE := t_prop(i).METEROUTREADERLOCCODE;    
      l_mo.METERREADFREQUENCY := t_prop(i).METERREADFREQUENCY;    

      IF t_prop(i).SAPEQUIPMENT IS NULL THEN
         IF l_no_equipment = 0 THEN 
            SELECT MAX(NO_EQUIPMENT) + 1
            INTO   l_no_equipment
            FROM   CIS.TVP063EQUIPMENT;
         ELSE
           l_no_equipment := l_no_equipment + 1;
         END IF;
         l_mo.METERREF := l_no_equipment;    
      ELSE
         l_mo.METERREF := t_prop(i).SAPEQUIPMENT;   
      END IF;
      
      l_mo.METERREMOVALREASON := null;    
       
      l_mo.METERTREATMENT := t_prop(i).METERTREATMENT;    
      l_mo.NUMBEROFDIGITS := t_prop(i).NUMBEROFDIGITS;    
      l_mo.OUTREADERID := t_prop(i).OUTREADERID;    
      l_mo.OUTREADERLOCFREEDES := t_prop(i).OUTREADERLOCFREEDES;    
      l_mo.OUTREADERPROTOCOL := t_prop(i).OUTREADERPROTOCOL;    
      l_mo.PHYSICALMETERSIZE := t_prop(i).PHYSICALMETERSIZE;    
      l_mo.REMOTEREADFLAG := t_prop(i).REMOTEREADFLAG;   
      l_mo.REMOTEREADTYPE := t_prop(i).REMOTEREADTYPE;    
      l_mo.RETURNTOSEWER := t_prop(i).RETURNTOSEWER;       
      l_mo.SEWCHARGEABLEMETERSIZE := t_prop(i).SEWCHARGEABLEMETERSIZE;        
      l_mo.SPID_PK := t_prop(i).SPID;       
      l_mo.WATERCHARGEMETERSIZE := t_prop(i).WATERCHARGEMETERSIZE;    
      l_mo.YEARLYVOLESTIMATE := t_prop(i).YEARLYVOLESTIMATE;   
      l_mo.NONMARKETMETERFLAG := t_prop(i).NONMARKETMETERFLAG;    
      l_mo.METERLOCATIONDESC := null;   
      l_mo.METERLOCSPECIALLOC := NULL;     
      l_mo.METERLOCSPECIALINSTR := NULL;    
      l_mo.MANUFCODE := null;    
     
      l_mo.INSTALLEDPROPERTYNUMBER := t_prop(i).STWPROPERTYNUMBER_PK;      --****  ????
      l_mo.SAPEQUIPMENT := t_prop(i).SAPEQUIPMENT;       
      --l_mo.MASTER_PROPERTY := t_prop(i).MASTER_PROPERTY;    
      l_mo.MASTER_PROPERTY := null;                                        --*** fix 
      l_mo.METER_MODEL := null;                                           
      l_mo.UNITOFMEASURE := null;    

     -- if water company Severn Trent must exist on lookup file.
      IF (    t_prop(i).OWC           = 'SEVERN' 
          AND l_mo.NONMARKETMETERFLAG = 0)
      THEN 
         l_progress := 'SELECT LU_SPID_RANGE';      
         BEGIN 
            SELECT SPID_PK 
            INTO  l_lu.SPID_PK
            FROM  LU_SPID_RANGE
            WHERE SPID_PK = t_prop(i).SPID;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Severn Trent SPID not in SPID range',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_exp := l_no_row_exp + 1;
         END;
      END IF;


      -- check SPID 
      
      l_progress := 'CHECK SPID AVAILABLE FOR MARKETABLE METER';
      IF (    l_mo.SPID_PK IS NULL
          AND l_mo.NONMARKETMETERFLAG = 0)
      THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('Invalid NULL SPID_PK on Marketable meter',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
          l_no_row_exp := l_no_row_exp + 1;
          l_rec_written := FALSE;
      END IF;    
          
      IF l_mo.SPID_PK IS NOT NULL THEN
          IF l_mo.METERTREATMENT = 'POTABLE' THEN 
             BEGIN
                SELECT COUNT(*)
                INTO   l_sc_count
                FROM   MO_SERVICE_COMPONENT
                WHERE  SPID_PK = l_mo.SPID_PK
                AND    SERVICECOMPONENTTYPE = 'MPW';
                  
                IF (l_sc_count = 0) THEN
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('NO MPW Service Component for POTABLE Meter',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                    l_no_row_exp := l_no_row_exp + 1;
                    l_rec_written := FALSE;
                END IF;
             END;
          ELSE 
              BEGIN
                SELECT SPID_PK
                INTO   l_dp.SPID_PK
                FROM   MO_DISCHARGE_POINT
                WHERE  SPID_PK = l_mo.SPID_PK
                AND    ROWNUM  = 1;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('NO Discharge Point for Private Meter',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                   l_no_row_exp := l_no_row_exp + 1;
                   l_rec_written := FALSE;
              END;
          END IF;
      END IF;

      -- check METER non market and market for correct GIS values

      IF l_mo.NONMARKETMETERFLAG = 1 THEN
         IF  (    l_mo.GPSX IS NOT NULL
              AND l_mo.GPSY IS NOT NULL)
         THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('GIS codes must be null for nonmarket meter',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
         END IF;
      ELSE
         l_progress := 'Validating GISX and GISY';
         l_gis_code := FN_VALIDATE_GIS(t_prop(i).GPSX || ';' || t_prop(i).GPSY);
            
         IF l_gis_code LIKE 'Invalid%' THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('Invalid GIS codes',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
         END IF;

      END IF;
            
          
     l_progress := 'INSERT MO_METER ';           
    
     IF l_rec_written THEN
        BEGIN 
          INSERT INTO MO_METER
           (COMBIMETERFLAG, DATALOGGERNONWHOLESALER,	DATALOGGERWHOLESALER,	FREEDESCRIPTOR,	GPSX,	GPSY,	MANUFACTURER_PK,	
            MANUFACTURERSERIALNUM_PK,	MDVOL,	MEASUREUNITATMETER,	MEASUREUNITFREEDESCRIPTOR, METERADDITIONREASON,	METERLOCATIONCODE,	
            METERLOCFREEDESCRIPTOR,	METERNETWORKASSOCIATION, METEROUTREADERGPSX,	METEROUTREADERGPSY,	METEROUTREADERLOCCODE,	
            METERREADFREQUENCY,	METERREF,	METERREMOVALREASON,  METERTREATMENT,	NUMBEROFDIGITS,	OUTREADERID,	OUTREADERLOCFREEDES,
            OUTREADERPROTOCOL, PHYSICALMETERSIZE,	REMOTEREADFLAG,	REMOTEREADTYPE, RETURNTOSEWER,	SEWCHARGEABLEMETERSIZE,	SPID_PK,	
            WATERCHARGEMETERSIZE,	YEARLYVOLESTIMATE, NONMARKETMETERFLAG , METERLOCATIONDESC, METERLOCSPECIALLOC, METERLOCSPECIALINSTR, 
            MANUFCODE, INSTALLEDPROPERTYNUMBER, SAPEQUIPMENT, MASTER_PROPERTY, METER_MODEL, UNITOFMEASURE, DPID_PK)
            VALUES
            (l_mo.COMBIMETERFLAG, l_mo.DATALOGGERNONWHOLESALER, l_mo.DATALOGGERWHOLESALER, l_mo.FREEDESCRIPTOR, l_mo.GPSX, l_mo.GPSY, l_mo.MANUFACTURER_PK, 
            l_mo.MANUFACTURERSERIALNUM_PK, l_mo.MDVOL, l_mo.MEASUREUNITATMETER, l_mo.MEASUREUNITFREEDESCRIPTOR, l_mo.METERADDITIONREASON, l_mo.METERLOCATIONCODE, 
            l_mo.METERLOCFREEDESCRIPTOR, l_mo.METERNETWORKASSOCIATION, l_mo.METEROUTREADERGPSX, l_mo.METEROUTREADERGPSY, l_mo.METEROUTREADERLOCCODE,
            l_mo.METERREADFREQUENCY, l_mo.METERREF, l_mo.METERREMOVALREASON, l_mo.METERTREATMENT, l_mo.NUMBEROFDIGITS, l_mo.OUTREADERID, l_mo.OUTREADERLOCFREEDES,
            l_mo.OUTREADERPROTOCOL, l_mo.PHYSICALMETERSIZE, l_mo.REMOTEREADFLAG, l_mo.REMOTEREADTYPE, l_mo.RETURNTOSEWER, l_mo.SEWCHARGEABLEMETERSIZE, l_mo.SPID_PK,
            l_mo.WATERCHARGEMETERSIZE, l_mo.YEARLYVOLESTIMATE, l_mo.NONMARKETMETERFLAG, l_mo.METERLOCATIONDESC, l_mo.METERLOCSPECIALLOC, l_mo.METERLOCSPECIALINSTR, 
            l_mo.MANUFCODE, l_mo.INSTALLEDPROPERTYNUMBER, l_mo.SAPEQUIPMENT, l_mo.MASTER_PROPERTY, l_mo.METER_MODEL, l_mo.UNITOFMEASURE, l_mo.DPID_PK );
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));  
             l_no_row_err := l_no_row_err + 1;
        END;
     ELSE
        l_no_row_dropped := l_no_row_dropped + 1;
     END IF;  

     IF l_rec_written THEN
        l_no_row_insert := l_no_row_insert + 1;
        IF t_prop(i).NONMARKETMETERFLAG = 0 THEN                             
           l_marketable_meter_cnt := l_marketable_meter_cnt + 1;            
           IF t_prop(i).SAPEQUIPMENT IS NULL THEN                             
              l_marketable_new_meter_cnt := l_marketable_new_meter_cnt + 1;  
           END IF;                                                           
        END IF;
        
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

     IF l_rec_written THEN   
         
        l_adr := NULL;
    
        l_no_cd_adr := l_no_cd_adr + 1; 
        l_adr.ADDRESS_PK := l_no_cd_adr; 
        l_progress := 'INSERT MO_ADDRESS';   

        l_adr.UPRN := NULL;   
        l_adr.PAFADDRESSKEY := t_prop(i).PAFADDRESSKEY;
        l_adr.PROPERTYNUMBERPROPERTY := NULL;
        l_adr.CUSTOMERNUMBERPROPERTY := NULL;
        l_adr.UPRNREASONCODE :=  NULL;    
         
        l_adr.SECONDADDRESABLEOBJECT := t_prop(i).SECONDADDRESABLEOBJECT;
        l_adr.PRIMARYADDRESSABLEOBJECT := t_prop(i).PRIMARYADDRESSABLEOBJECT;      
        l_adr.ADDRESSLINE01 := t_prop(i).ADDRESSLINE01;
        l_adr.ADDRESSLINE02 := t_prop(i).ADDRESSLINE02;
        l_adr.ADDRESSLINE03 := t_prop(i).ADDRESSLINE03;
        l_adr.ADDRESSLINE04 := t_prop(i).ADDRESSLINE04;
        l_adr.ADDRESSLINE05 := t_prop(i).ADDRESSLINE05;
        l_adr.POSTCODE := t_prop(i).POSTCODE;
        l_adr.COUNTRY := null;
        l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(i).LOCATIONFREETEXTDESCRIPTOR;
         
        BEGIN 
           INSERT INTO MO_ADDRESS
           (ADDRESS_PK, UPRN, PAFADDRESSKEY, PROPERTYNUMBERPROPERTY, CUSTOMERNUMBERPROPERTY, 
            UPRNREASONCODE, SECONDADDRESABLEOBJECT, PRIMARYADDRESSABLEOBJECT, ADDRESSLINE01, 
            ADDRESSLINE02, ADDRESSLINE03, ADDRESSLINE04, ADDRESSLINE05, POSTCODE,
            COUNTRY, LOCATIONFREETEXTDESCRIPTOR)
           VALUES
           (l_adr.ADDRESS_PK, l_adr.UPRN, l_adr.PAFADDRESSKEY, l_adr.PROPERTYNUMBERPROPERTY, l_adr.CUSTOMERNUMBERPROPERTY, 
            l_adr.UPRNREASONCODE, l_adr.SECONDADDRESABLEOBJECT, l_adr.PRIMARYADDRESSABLEOBJECT, l_adr.ADDRESSLINE01, 
            l_adr.ADDRESSLINE02, l_adr.ADDRESSLINE03, l_adr.ADDRESSLINE04, l_adr.ADDRESSLINE05, l_adr.POSTCODE,
            l_adr.COUNTRY, l_adr.LOCATIONFREETEXTDESCRIPTOR);
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped_cd_adr := l_no_row_dropped_cd_adr + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_err := l_no_row_err + 1;
        END;

        IF l_rec_written THEN
           l_no_row_insert_adr := l_no_row_insert_adr + 1;
        ELSE 
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
        END IF;
  
        l_prev_floc := l_adr.ADDRESS_PK;
            
         -- Write METER ADDRESS
  
         l_adr_mt := NULL;
         l_adr_mt.ADDRESS_PK := l_adr.ADDRESS_PK;
         l_adr_mt.METERSERIALNUMBER_PK := l_mo.MANUFACTURERSERIALNUM_PK;
         l_adr_mt.INSTALLEDPROPERTYNUMBER := l_mo.INSTALLEDPROPERTYNUMBER;    
         l_adr_mt.MANUFACTURER_PK := l_mo.MANUFACTURER_PK;
         l_adr_mt.ADDRESSUSAGEPROPERTY := 'SitedAt'; 
         l_adr_mt.EFFECTIVEFROMDATE := SYSDATE;             
         l_adr_mt.EFFECTIVETODATE := NULL;
         l_adr_mt.MANUFCODE := l_mo.MANUFCODE;
   
         l_progress := 'INSERT MO_METER_ADDRESS';     
         BEGIN 
            INSERT INTO MO_METER_ADDRESS 
            (ADDRESSPROPERTY_PK, METERSERIALNUMBER_PK, ADDRESS_PK, ADDRESSUSAGEPROPERTY, 
             EFFECTIVEFROMDATE, EFFECTIVETODATE, MANUFACTURER_PK, INSTALLEDPROPERTYNUMBER, MANUFCODE)
            VALUES
            (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_mt.METERSERIALNUMBER_PK, l_adr_mt.ADDRESS_PK,  l_adr_mt.ADDRESSUSAGEPROPERTY,
             l_adr_mt.EFFECTIVEFROMDATE, l_adr_mt.EFFECTIVETODATE, l_adr_mt.MANUFACTURER_PK, l_adr_mt.INSTALLEDPROPERTYNUMBER, l_adr_mt.MANUFCODE);
         EXCEPTION 
         WHEN OTHERS THEN
              l_no_row_dropped_prop := l_no_row_dropped_prop + 1;
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_err := l_no_row_err + 1;
         END;

         -- keep count of records written
         IF l_rec_written THEN
            l_no_row_insert_adrprop := l_no_row_insert_adrprop + 1;
         ELSE 
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
         END IF;  

     END IF;
     
     l_prev_prp := t_prop(i).STWPROPERTYNUMBER_PK;
      
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
  
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP32', 1090, l_no_row_read,    'Distinct Eligible Meters read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP32', 1100, l_no_row_dropped, 'Eligible Meters  dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1110, l_no_row_insert,  'Eligible Meters written to MO_METER during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1111, l_marketable_meter_cnt,  'Eligible Marketable Meters written to MO_METER during Transform'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1112, l_marketable_new_meter_cnt,  'Eligible New Marketable Meters written to MO_METER during Transform'); 
 
  l_job.ind_status := 'END';
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
END P_FIN_TRAN_METER;
/
show error;

exit;