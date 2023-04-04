CREATE OR REPLACE PROCEDURE P_OWC_TRAN_METER(no_batch          IN MIG_BATCHSTATUS.no_batch%TYPE,
                                             no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                             return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_OWC_TRAN_METER.sql
--
-- Subversion $Revision: 6483 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER from OWC supplied data 
--               - OWC_METER.
--
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-- V 0.02      11/10/2016  K.Burton   Changes to process DWRCYMRU-W data and add reconciliations
-- V 0.03      20/10/2016  K.Burton   Split main cursor by OWC
-- V 0.04      07/12/2016  D.Cheung   Fix to Recon Point 1111
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_METER';
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
  l_sc_count                    NUMBER;
  l_gis_code                    VARCHAR2(60);
  l_no_equipment                NUMBER := 0;
  l_marketable_meter_cnt        NUMBER := 0;
  l_marketable_new_meter_cnt    NUMBER := 0;
  l_count                       NUMBER;
  l_add_new_address             BOOLEAN;
  
  l_owc_measure                 LU_OWC_RECON_MEASURES%ROWTYPE;
   
  l_no_row_read_mtr             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER meter records read from OWC_METER per OWC
  l_no_row_dropped_mtr          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER meter records dropped from OWC_METER per OWC
  l_no_row_insert_mtr           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER records inserted from OWC_METER per OWC

  l_no_row_read_adr             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS meter records read from OWC_METER per OWC
  l_no_row_dropped_adr          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS meter records dropped from OWC_METER per OWC
  l_no_row_insert_adr           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS records inserted from OWC_METER per OWC

  l_no_row_read_adrm            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER_ADDRESS records read from OWC_METER per OWC
  l_no_row_dropped_adrm         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER_ADDRESS records dropped from OWC_METER per OWC
  l_no_row_insert_adrm          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_METER_ADDRESS records inserted from OWC_METER per OWC  

  
  owc_meter_exception EXCEPTION;
  owc_meter_address_exception EXCEPTION;
  owc_over_tolerance_exception EXCEPTION;  
  
CURSOR cur_prop (p_property_start   MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_property_end     MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_owc              VARCHAR2) IS 
    SELECT  OM.MANUFACTURERSERIALNUM_PK,
            OM.MANUFACTURER_PK,
            OM.NUMBEROFDIGITS,
            OM.MEASUREUNITATMETER,
            OM.MEASUREUNITFREEDESCRIPTOR,
            OM.PHYSICALMETERSIZE,
            OM.METERREADFREQUENCY,
            OM.INITIALMETERREADDATE,
            CASE WHEN OM.METERTREATMENT = 'CROSSBORDER'
              THEN NULL
              ELSE OM.RETURNTOSEWER
            END AS RETURNTOSEWER,
            CASE WHEN OM.METERTREATMENT NOT IN ('POTABLE','NONPOTABLE')
              THEN 0
              ELSE OM.WATERCHARGEMETERSIZE
            END AS WATERCHARGEMETERSIZE,
            CASE WHEN OM.METERTREATMENT = 'CROSSBORDER'
              THEN NULL
              ELSE OM.SEWCHARGEABLEMETERSIZE
            END AS SEWCHARGEABLEMETERSIZE,
            OM.DATALOGGERWHOLESALER,
            OM.DATALOGGERNONWHOLESALER,
            OM.GPSX,
            OM.GPSY,
            OM.METERLOCFREEDESCRIPTOR,
            OM.METEROUTREADERGPSX,
            OM.METEROUTREADERGPSY,
            OM.OUTREADERLOCFREEDES,
            OM.METEROUTREADERLOCCODE,
            OM.METERTREATMENT,
            OM.METERLOCATIONCODE,
            OM.COMBIMETERFLAG,
            OM.YEARLYVOLESTIMATE,
            OM.REMOTEREADFLAG,
            OM.REMOTEREADTYPE,
            OM.OUTREADERID,
            OM.OUTREADERPROTOCOL,
            OM.LOCATIONFREETEXTDESCRIPTOR,
            OM.SECONDADDRESABLEOBJECT,
            OM.PRIMARYADDRESSABLEOBJECT,
            OM.ADDRESSLINE01,
            OM.ADDRESSLINE02,
            OM.ADDRESSLINE03,
            OM.ADDRESSLINE04,
            OM.ADDRESSLINE05,
            OM.POSTCODE,
            OM.PAFADDRESSKEY,
            MSP.SPID_PK SPID,
            OM.NONMARKETMETERFLAG,
            OM.SAPEQUIPMENT,
            OM.SAPFLOCNUMBER,
            OM.OWC,
            MEP.STWPROPERTYNUMBER_PK,
            MPA.ADDRESS_PK
    FROM    RECEPTION.OWC_METER OM,
--            MO_ELIGIBLE_PREMISES MEP,
            MO_ELIGIBLE_PREMISES MEP,
--            MO_PROPERTY_ADDRESS MPA,
            MO_PROPERTY_ADDRESS MPA,
            RECEPTION.OWC_METER_SUPPLY_POINT MSP
    WHERE OM.MANUFACTURER_PK = MSP.MANUFACTURER_PK
    AND OM.MANUFACTURERSERIALNUM_PK = MSP.MANUFACTURERSERIALNUM_PK
    AND OM.OWC = MSP.OWC
    AND OM.OWC = p_owc
    AND MEP.CORESPID_PK(+) = SUBSTR(MSP.SPID_PK,1,10) 
    AND MEP.STWPROPERTYNUMBER_PK = MPA.STWPROPERTYNUMBER_PK(+)
    ORDER BY MEP.STWPROPERTYNUMBER_PK; 

  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM RECEPTION.OWC_METER;
--    WHERE OWC = 'DWRCYMRU-W';
    
  TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
  t_prop  tab_property;
  
  FUNCTION GET_OWC_MEASURES (p_owc VARCHAR2, p_table VARCHAR2) RETURN LU_OWC_RECON_MEASURES%ROWTYPE IS
    l_owc_measure LU_OWC_RECON_MEASURES%ROWTYPE;
  BEGIN
    SELECT * INTO l_owc_measure
    FROM LU_OWC_RECON_MEASURES 
    WHERE OWC = p_owc
    AND MO_TABLE = p_table;
    
    RETURN l_owc_measure;
  END GET_OWC_MEASURES;  

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
  
  FOR owc in owc_cur
  LOOP
  
    l_no_row_read_mtr := 0;
    l_no_row_dropped_mtr := 0;
    l_no_row_insert_mtr := 0;

    l_no_row_read_adr := 0;
    l_no_row_dropped_adr := 0;
    l_no_row_insert_adr := 0;

    l_no_row_read_adrm := 0;
    l_no_row_dropped_adrm := 0;
    l_no_row_insert_adrm := 0;
    
    -- start processing all records for range supplied
    OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX, owc.OWC);
    LOOP
    
      FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
   
      FOR i IN 1..t_prop.COUNT
      LOOP
        l_err.TXT_KEY := t_prop(i).MANUFACTURER_PK || ',' || t_prop(i).MANUFACTURERSERIALNUM_PK;
        l_progress := 'Set up values';          
        l_mo := NULL;
        
        l_no_row_read := l_no_row_read + 1;
        l_no_row_read_mtr := l_no_row_read_mtr + 1;
        
        BEGIN
          IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
            RAISE owc_over_tolerance_exception;
          END IF;
          
          -- if SPID is NULL then reject the record - we can't have a meter with no SPID
          IF t_prop(i).SPID IS NULL THEN
            l_error_message := 'Cannot add a meter with no SPID';
            RAISE owc_meter_exception;
          ELSE
            -- if the SPID provided is a water SPID then reject - we cannoty load OWC water SPIDs
            IF SUBSTR(t_prop(i).SPID,11,1) = 'W' THEN
              l_error_message := 'Cannot add a water meter for OWC '  || t_prop(i).OWC;
              RAISE owc_meter_exception;
            ELSE -- if we get here we should only have sewerage meter to add - should only happen for DWRCYMRU
              l_mo.COMBIMETERFLAG := t_prop(i).COMBIMETERFLAG;     
              l_mo.DATALOGGERNONWHOLESALER := t_prop(i).DATALOGGERNONWHOLESALER;
              l_mo.DATALOGGERWHOLESALER := t_prop(i).DATALOGGERWHOLESALER; 
              l_mo.FREEDESCRIPTOR := NULL;                ---- fix  ********  
              l_mo.GPSX := t_prop(i).GPSX;  
              l_mo.GPSY := t_prop(i).GPSY;        
              l_mo.MANUFACTURER_PK := t_prop(i).MANUFACTURER_PK;          
              l_mo.MANUFACTURERSERIALNUM_PK := t_prop(i).MANUFACTURERSERIALNUM_PK;   
              l_mo.MDVOL := NULL;    
              l_mo.DPID_PK := NULL;
              l_mo.MEASUREUNITATMETER := t_prop(i).MEASUREUNITATMETER;    
              l_mo.MEASUREUNITFREEDESCRIPTOR := t_prop(i).MEASUREUNITFREEDESCRIPTOR;    
              l_mo.METERADDITIONREASON := NULL;      
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
                    INTO l_no_equipment
                    FROM CIS.TVP063EQUIPMENT;
                 ELSE
                   l_no_equipment := l_no_equipment + 1;
                 END IF;
                 l_mo.METERREF := l_no_equipment;    
              ELSE
                 l_mo.METERREF := t_prop(i).SAPEQUIPMENT;   
              END IF;
              
              l_mo.METERREMOVALREASON := NULL;    
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
              l_mo.NONMARKETMETERFLAG := NVL(t_prop(i).NONMARKETMETERFLAG,0);    
              l_mo.METERLOCATIONDESC := NULL;   
              l_mo.METERLOCSPECIALLOC := NULL;     
              l_mo.METERLOCSPECIALINSTR := NULL;    
              l_mo.MANUFCODE := NULL;    
              l_mo.INSTALLEDPROPERTYNUMBER := t_prop(i).STWPROPERTYNUMBER_PK;      --****  ????
              l_mo.SAPEQUIPMENT := t_prop(i).SAPEQUIPMENT;       
              l_mo.MASTER_PROPERTY := NULL;                                        --*** fix 
              l_mo.METER_MODEL := NULL;                                           
              l_mo.UNITOFMEASURE := NULL;    
        
              l_progress := 'Validate SPID'; 
              
              IF owc.OWC <> 'DWRCYMRU-W' THEN
                SELECT COUNT(*) 
                INTO l_count
                FROM LU_SPID_RANGE 
                WHERE SPID_PK = t_prop(i).SPID;
             
                IF l_count > 0 THEN
                  l_error_message := 'Severn Trent SPID provided for OWC ' || t_prop(i).OWC;
                  RAISE owc_meter_exception;
                END IF;
              END IF;
              
              -- for DWRCYMRU case METERTREATMENT should be CROSSBORDER
              IF owc.OWC = 'DWRCYMRU-W' THEN -- check value being inserted here
                l_mo.METERTREATMENT := 'CROSSBORDER';
              ELSE
                IF l_mo.METERTREATMENT IN ('CROSSBORDER','POTABLE','NONPOTABLE') THEN
                  l_error_message := 'Invalid METERTREATMENT value';
                  RAISE owc_meter_exception;
                END IF;
              END IF;
              
              l_progress := 'Validate Service Components'; 
    
              SELECT COUNT(*)
              INTO   l_count
              FROM   MO_SERVICE_COMPONENT
              WHERE  SPID_PK = l_mo.SPID_PK
              AND    SERVICECOMPONENTTYPE = 'MS';
                    
              IF l_count = 0 THEN
                l_error_message := 'No MS Service Component for Sewerage Meter';
                RAISE owc_meter_exception;
              END IF;
    
--              l_progress := 'Validate Discharge Points'; 
--            
--              SELECT COUNT(*)
--              INTO   l_count
--              FROM   MO_DISCHARGE_POINT
--              WHERE  SPID_PK = l_mo.SPID_PK
--              AND    ROWNUM  = 1;
--    
--              IF l_count = 0 THEN
--                l_error_message := 'No Discharge Point for Private Meter';
--                RAISE owc_meter_exception;
--              END IF;
        
              l_progress := 'Validating GISX and GISY';
              
              l_gis_code := FN_VALIDATE_GIS(t_prop(i).GPSX || ';' || t_prop(i).GPSY);
                  
              IF l_gis_code LIKE 'Invalid%' THEN
                l_error_message := 'Invalid GIS codes';
                RAISE owc_meter_exception;
              END IF; 
                         
             l_progress := 'INSERT into MO_METER ';           
            
              BEGIN 
                INSERT INTO MO_METER
                 (COMBIMETERFLAG, DATALOGGERNONWHOLESALER,	DATALOGGERWHOLESALER,	FREEDESCRIPTOR,	GPSX,	GPSY,	MANUFACTURER_PK,	
                  MANUFACTURERSERIALNUM_PK,	MDVOL,	MEASUREUNITATMETER,	MEASUREUNITFREEDESCRIPTOR, METERADDITIONREASON,	METERLOCATIONCODE,	
                  METERLOCFREEDESCRIPTOR,	METERNETWORKASSOCIATION, METEROUTREADERGPSX,	METEROUTREADERGPSY,	METEROUTREADERLOCCODE,	
                  METERREADFREQUENCY,	METERREF,	METERREMOVALREASON,  METERTREATMENT,	NUMBEROFDIGITS,	OUTREADERID,	OUTREADERLOCFREEDES,
                  OUTREADERPROTOCOL, PHYSICALMETERSIZE,	REMOTEREADFLAG,	REMOTEREADTYPE, RETURNTOSEWER,	SEWCHARGEABLEMETERSIZE,	SPID_PK,	
                  WATERCHARGEMETERSIZE,	YEARLYVOLESTIMATE, NONMARKETMETERFLAG , METERLOCATIONDESC, METERLOCSPECIALLOC, METERLOCSPECIALINSTR, 
                  MANUFCODE, INSTALLEDPROPERTYNUMBER, SAPEQUIPMENT, MASTER_PROPERTY, METER_MODEL, UNITOFMEASURE, DPID_PK, OWC)
                  VALUES
                  (l_mo.COMBIMETERFLAG, l_mo.DATALOGGERNONWHOLESALER, l_mo.DATALOGGERWHOLESALER, l_mo.FREEDESCRIPTOR, l_mo.GPSX, l_mo.GPSY, l_mo.MANUFACTURER_PK, 
                  l_mo.MANUFACTURERSERIALNUM_PK, l_mo.MDVOL, l_mo.MEASUREUNITATMETER, l_mo.MEASUREUNITFREEDESCRIPTOR, l_mo.METERADDITIONREASON, l_mo.METERLOCATIONCODE, 
                  l_mo.METERLOCFREEDESCRIPTOR, l_mo.METERNETWORKASSOCIATION, l_mo.METEROUTREADERGPSX, l_mo.METEROUTREADERGPSY, l_mo.METEROUTREADERLOCCODE,
                  l_mo.METERREADFREQUENCY, l_mo.METERREF, l_mo.METERREMOVALREASON, l_mo.METERTREATMENT, l_mo.NUMBEROFDIGITS, l_mo.OUTREADERID, l_mo.OUTREADERLOCFREEDES,
                  l_mo.OUTREADERPROTOCOL, l_mo.PHYSICALMETERSIZE, l_mo.REMOTEREADFLAG, l_mo.REMOTEREADTYPE, l_mo.RETURNTOSEWER, l_mo.SEWCHARGEABLEMETERSIZE, l_mo.SPID_PK,
                  l_mo.WATERCHARGEMETERSIZE, l_mo.YEARLYVOLESTIMATE, l_mo.NONMARKETMETERFLAG, l_mo.METERLOCATIONDESC, l_mo.METERLOCSPECIALLOC, l_mo.METERLOCSPECIALINSTR, 
                  l_mo.MANUFCODE, l_mo.INSTALLEDPROPERTYNUMBER, l_mo.SAPEQUIPMENT, l_mo.MASTER_PROPERTY, l_mo.METER_MODEL, l_mo.UNITOFMEASURE, l_mo.DPID_PK, owc.OWC );
                  
                l_no_row_insert := l_no_row_insert + 1;
                l_no_row_insert_mtr := l_no_row_insert_mtr + 1;
                
                IF l_mo.NONMARKETMETERFLAG = 0 THEN                             
                   l_marketable_meter_cnt := l_marketable_meter_cnt + 1;            
                   IF t_prop(i).SAPEQUIPMENT IS NULL THEN                             
                      l_marketable_new_meter_cnt := l_marketable_new_meter_cnt + 1;  
                   END IF;                                                           
                END IF;
                
              EXCEPTION 
                WHEN OTHERS THEN 
                  l_error_number := SQLCODE;
                  l_error_message := SQLERRM;
                  l_no_row_err := l_no_row_err + 1;
                  RAISE owc_meter_exception;
              END;
            END IF; -- SPID is not a water SPID
          END IF; -- SPID is not NULL
          
            l_progress := 'INSERT into MO_ADDRESS '; 
            
            l_no_row_read_adr := l_no_row_read_adr + 1;
            l_no_row_read_adrm := l_no_row_read_adrm + 1;
            
            -- if we have no address from main query we need to add one
            IF t_prop(i).ADDRESS_PK IS NULL THEN
              l_add_new_address := TRUE;
            ELSE -- if we have a property address key from main query check that the address is the same as the meter
              SELECT PRIMARYADDRESSABLEOBJECT,
                     SECONDADDRESABLEOBJECT,
                     ADDRESSLINE01,
                     ADDRESSLINE02,
                     ADDRESSLINE03,
                     ADDRESSLINE04,
                     ADDRESSLINE05,
                     POSTCODE
              INTO l_adr.PRIMARYADDRESSABLEOBJECT,
                   l_adr.SECONDADDRESABLEOBJECT,
                   l_adr.ADDRESSLINE01,
                   l_adr.ADDRESSLINE02,
                   l_adr.ADDRESSLINE03,
                   l_adr.ADDRESSLINE04,
                   l_adr.ADDRESSLINE05,
                   l_adr.POSTCODE
              FROM MO_ADDRESS
              WHERE ADDRESS_PK = t_prop(i).ADDRESS_PK;
              
              IF (t_prop(i).PRIMARYADDRESSABLEOBJECT = l_adr.PRIMARYADDRESSABLEOBJECT AND
                  t_prop(i).SECONDADDRESABLEOBJECT = l_adr.SECONDADDRESABLEOBJECT AND
                  t_prop(i).ADDRESSLINE01 = l_adr.ADDRESSLINE01 AND
                  t_prop(i).ADDRESSLINE02 = l_adr.ADDRESSLINE02 AND
                  t_prop(i).ADDRESSLINE03 = l_adr.ADDRESSLINE03 AND
                  t_prop(i).ADDRESSLINE04 = l_adr.ADDRESSLINE04 AND
                  t_prop(i).ADDRESSLINE05 = l_adr.ADDRESSLINE05 AND
                  t_prop(i).POSTCODE = l_adr.POSTCODE) THEN
                l_add_new_address := FALSE;
              ELSE
                l_add_new_address := TRUE;
              END IF;
                  
            END IF;
            
            IF l_add_new_address THEN
              l_adr := NULL;
        
              SELECT MAX(ADDRESS_PK)+1 INTO l_adr.ADDRESS_PK FROM MO_ADDRESS;
              
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
              l_adr.COUNTRY := NULL;
              l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(i).LOCATIONFREETEXTDESCRIPTOR;
             
              BEGIN 
                 INSERT INTO MO_ADDRESS
                 (ADDRESS_PK, UPRN, PAFADDRESSKEY, PROPERTYNUMBERPROPERTY, CUSTOMERNUMBERPROPERTY, 
                  UPRNREASONCODE, SECONDADDRESABLEOBJECT, PRIMARYADDRESSABLEOBJECT, ADDRESSLINE01, 
                  ADDRESSLINE02, ADDRESSLINE03, ADDRESSLINE04, ADDRESSLINE05, POSTCODE,
                  COUNTRY, LOCATIONFREETEXTDESCRIPTOR, OWC)
                 VALUES
                 (l_adr.ADDRESS_PK, l_adr.UPRN, l_adr.PAFADDRESSKEY, l_adr.PROPERTYNUMBERPROPERTY, l_adr.CUSTOMERNUMBERPROPERTY, 
                  l_adr.UPRNREASONCODE, l_adr.SECONDADDRESABLEOBJECT, l_adr.PRIMARYADDRESSABLEOBJECT, l_adr.ADDRESSLINE01, 
                  l_adr.ADDRESSLINE02, l_adr.ADDRESSLINE03, l_adr.ADDRESSLINE04, l_adr.ADDRESSLINE05, l_adr.POSTCODE,
                  l_adr.COUNTRY, l_adr.LOCATIONFREETEXTDESCRIPTOR, owc.OWC);
                  
                  l_no_row_insert_adr := l_no_row_insert_adr + 1;
              EXCEPTION 
                WHEN OTHERS THEN 
                  l_error_number := SQLCODE;
                  l_error_message := SQLERRM;
--                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  RAISE owc_meter_address_exception;
              END;
            
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
                 EFFECTIVEFROMDATE, EFFECTIVETODATE, MANUFACTURER_PK, INSTALLEDPROPERTYNUMBER, MANUFCODE, OWC)
                VALUES
                (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_mt.METERSERIALNUMBER_PK, l_adr_mt.ADDRESS_PK,  l_adr_mt.ADDRESSUSAGEPROPERTY,
                 l_adr_mt.EFFECTIVEFROMDATE, l_adr_mt.EFFECTIVETODATE, l_adr_mt.MANUFACTURER_PK, l_adr_mt.INSTALLEDPROPERTYNUMBER, l_adr_mt.MANUFCODE, owc.OWC);
                 
                l_no_row_insert_adrm := l_no_row_insert_adrm + 1; 
             EXCEPTION 
              WHEN OTHERS THEN
                l_error_number := SQLCODE;
                l_error_message := SQLERRM;
--                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                RAISE owc_meter_address_exception;
             END;
            END IF; -- if l_add_new_address
    
        EXCEPTION
          WHEN owc_meter_exception THEN
            l_no_row_exp := l_no_row_exp + 1;
            l_no_row_dropped := l_no_row_dropped + 1;
            l_no_row_dropped_mtr := l_no_row_dropped_mtr + 1;
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
          WHEN owc_meter_address_exception THEN
            l_no_row_exp := l_no_row_exp + 1;
            l_no_row_dropped_adr := l_no_row_dropped_adr + 1;
            l_no_row_dropped_adrm := l_no_row_dropped_adrm + 1;
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
          WHEN owc_over_tolerance_exception THEN
            l_job.IND_STATUS := 'ERR';
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded - Dropping bad data',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
            P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
            COMMIT;
            return_code := -1;
            RETURN; -- quit proc       
        END;          
      END LOOP; -- cur_prop
      
      IF t_prop.COUNT < l_job.NO_COMMIT THEN
         EXIT;
      ELSE
         COMMIT;
      END IF;
       
    END LOOP;
    
    CLOSE cur_prop;  

    -- write OWC specific counts 
    l_progress := 'Writing OWC counts ' || owc.OWC;  
    
    -- meter
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_METER');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_mtr, owc.OWC || ' Meters read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_mtr, owc.OWC || ' Meters dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_mtr, owc.OWC || ' Meters written to MO_METER during Transform'); 
    
    -- address
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_ADDRESS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_adr, owc.OWC || ' Addresses read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_adr, owc.OWC || ' Addresses dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_adr, owc.OWC || ' Addresses written to MO_ADDRESS during Transform'); 

    -- meter address
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_METER_ADDRESS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_adrm, owc.OWC || ' Meter Addresses read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_adrm, owc.OWC || ' Meter Addresses dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_adrm, owc.OWC || 'Meter Addresses written to MO_METER_ADDRESS during Transform'); 
  END LOOP; -- owc_cur
  -- write counts 
  l_progress := 'Writing Total Counts';  
  
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
END P_OWC_TRAN_METER;
/
show error;

exit;