create or replace
PROCEDURE P_OWC_SAP_SUPPLY_POINT(no_batch          IN MIG_BATCHSTATUS.no_batch%TYPE,
                                                    no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                    return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: OWC Supply Point
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_OWC_SAP_SUPPLY_POINT.sql
--
-- Subversion $Revision: 6413 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_ELIGIBLE_PREMISES, MO_CUSTOMER, SAP_CUSTOMER_ADDRESS,
--                  SAP_ADDRESS, MO_SUPPLY_POINT from OWC supplied data - OWC_SUPPLY_POINT
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      12/10/2016  K.Burton   Initial Draft
-- V 0.02      19/10/2016  K.Burton   Added check for TE SPIDs to exclude fro phase 2
-- V 0.03      19/10/2016  S.Badhan   For SPID range check do not check if existing property number
-- V 0.04      20/10/2016  S.Badhan   Create new property if property number supplied does not exist
-- V 0.05      24/10/2016  K.Burton   Check property against ELIGIBILITY_CONTROL_TABLE
-- V 0.06      25/10/2016  K.Burton   Check for STW SPIDs excludes Supply Points with NOSPID reason code
-- V 0.07      08/11/2016  K.Burton   Removed work-around for missing FLOC numbers
-- V 0.08      10/11/2016  K.Burton   Restricted main cursor to exclude SOUTHSTAFFS-W "spare" SPIDs from
--                                    LU_OWC_SSW_SPIDS
-- V 0.09      23/11/2016  K.Burton   Removed check on LU_SPID_RANGE
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_SAP_SUPPLY_POINT';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE := 0; 
  l_prev_cus                    MO_CUSTOMER.CUSTOMERNUMBER_PK%TYPE := 0;  
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_lu                          LU_SPID_RANGE%ROWTYPE;  
  l_sp                          MO_SUPPLY_POINT%ROWTYPE; 
  l_pr                          MO_ELIGIBLE_PREMISES%ROWTYPE;  
  l_cus                         MO_CUSTOMER%ROWTYPE;  
  l_adr                         SAP_ADDRESS%ROWTYPE;  
  l_adr_pr                      SAP_PROPERTY_ADDRESS%ROWTYPE := NULL;  
  l_adr_cus                     SAP_CUST_ADDRESS%Rowtype := Null;  
  l_floc                        LU_SAP_FLOCA%ROWTYPE;      

  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- total rows read from OWC_SUPPLY_POINT
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- total rows dropped from OWC_SUPPLY_POINT
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- total rows written from OWC_SUPPLY_POINT

  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE := 0;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE := 0;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE := 0;
  l_rec_written                 BOOLEAN;
  l_create_prop                 BOOLEAN;  
  l_no_property                 NUMBER(9) := 0;  
  l_no_customer                 NUMBER(9) := 0;    
  l_no_cd_adr                   NUMBER(9) := 0;
  
  l_owc_measure                 LU_OWC_RECON_MEASURES%ROWTYPE;
  l_count                       NUMBER;
  -- reconciliation counts
  l_no_row_read_prem            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ELIGIBLE_PREMISES records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_prem         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ELIGIBLE_PREMISES records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_prem          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ELIGIBLE_PREMISES records inserted from OWC_SUPPLY_POINT per OWC
  
  l_no_row_read_cus             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_cus          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_cus           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER records inserted from OWC_SUPPLY_POINT per OWC

  l_no_row_read_sp              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_SUPPLY_POINT records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_sp           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_SUPPLY_POINT records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_sp            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_SUPPLY_POINT records inserted from OWC_SUPPLY_POINT per OWC
  l_no_row_update_sp            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_SUPPLY_POINT records updated from OWC_SUPPLY_POINT per OWC
  
  l_no_row_read_padr            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS Property records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_padr         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS Property records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_read_cadr            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS Customer records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_cadr         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS Customer records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_adr           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_ADDRESS records inserted from OWC_SUPPLY_POINT per OWC

  l_no_row_read_adrp            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_PROPERTY_ADDRESS records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_adrp         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_PROPERTY_ADDRESS records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_adrp          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_PROPERTY_ADDRESS records inserted from OWC_SUPPLY_POINT per OWC

  l_no_row_read_adrc            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER_ADDRESS records read from OWC_SUPPLY_POINT per OWC
  l_no_row_dropped_adrc         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER_ADDRESS records dropped from OWC_SUPPLY_POINT per OWC
  l_no_row_insert_adrc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  -- MO_CUSTOMER_ADDRESS records inserted from OWC_SUPPLY_POINT per OWC

  -- reconciliation totals
  l_tot_row_read_prem            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_ELIGIBLE_PREMISES records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_prem         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_ELIGIBLE_PREMISES records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_prem          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_ELIGIBLE_PREMISES records inserted from OWC_SUPPLY_POINT 
  
  l_tot_row_read_cus             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_cus          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_cus           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER records inserted from OWC_SUPPLY_POINT 

  l_tot_row_read_sp              mig_cplog.recon_measure_total%type := 0; -- Total MO_SUPPLY_POINT records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_sp           mig_cplog.recon_measure_total%type := 0; -- Total MO_SUPPLY_POINT records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_sp            mig_cplog.recon_measure_total%type := 0; -- Total MO_SUPPLY_POINT records inserted from OWC_SUPPLY_POINT 

  l_tot_row_read_adr             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_ADDRESS records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_adr          MIG_CPLOG.RECON_MEASURE_TOTAL%type := 0; -- Total MO_ADDRESS records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_adr           MIG_CPLOG.RECON_MEASURE_TOTAL%type := 0; -- Total MO_ADDRESS records inserted from OWC_SUPPLY_POINT 

  l_tot_row_read_adrp            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_PROPERTY_ADDRESS records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_adrp         MIG_CPLOG.RECON_MEASURE_TOTAL%type := 0; -- Total MO_PROPERTY_ADDRESS records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_adrp          MIG_CPLOG.RECON_MEASURE_TOTAL%type := 0; -- Total MO_PROPERTY_ADDRESS records inserted from OWC_SUPPLY_POINT 

  l_tot_row_read_adrc            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER_ADDRESS records read from OWC_SUPPLY_POINT 
  l_tot_row_dropped_adrc         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER_ADDRESS records dropped from OWC_SUPPLY_POINT 
  l_tot_row_insert_adrc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; -- Total MO_CUSTOMER_ADDRESS records inserted from OWC_SUPPLY_POINT 
  
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE,
                 p_owc VARCHAR2)                 
   IS 
    SELECT  SPID_PK,
            WHOLESALERID,
            RETAILERID,
            SERVICECATEGORY,
            SUPPLYPOINTEFFECTIVEFROMDATE ,
            PAIRINGREFREASONCODE,
            OTHERWHOLESALERID,
            MULTIPLEWHOLESALERFLAG,
            DISCONRECONDEREGSTATUS,
            VOABAREFERENCE,
            VOABAREFRSNCODE,
            UPRN,
            UPRNREASONCODE,
            CUSTOMERCLASSIFICATION,
            PUBHEALTHRELSITEARR,
            NONPUBHEALTHRELSITE,
            NONPUBHEALTHRELSITEDSC,
            STDINDUSTRYCLASSCODE,
            STDINDUSTRYCLASSCODETYPE,
            RATEABLEVALUE,
            OCCUPENCYSTATUS,
            BUILDINGWATERSTATUS,
            LANDLORDSPID,
            SECTION154,
            CUSTOMERNAME,
            CUSTOMERBANNERNAME,
            PREMLOCATIONFREETEXTDESCRIPTOR,
            PREMSECONDADDRESABLEOBJECT,
            PREMPRIMARYADDRESSABLEOBJECT,
            PREMADDRESSLINE01,
            PREMADDRESSLINE02,
            PREMADDRESSLINE03,
            PREMADDRESSLINE04,
            PREMADDRESSLINE05,
            PREMPOSTCODE,
            PREMPAFADDRESSKEY,
            CUSTLOCATIONFREETEXTDESCRIPTOR,
            CUSTSECONDADDRESABLEOBJECT,
            CUSTPRIMARYADDRESSABLEOBJECT,
            CUSTADDRESSLINE01,
            CUSTADDRESSLINE02,
            CUSTADDRESSLINE03,
            CUSTADDRESSLINE04,
            CUSTADDRESSLINE05,
            CUSTPOSTCODE,
            CUSTCOUNTRY,
            CUSTPAFADDRESSKEY,
            STWPROPERTYNUMBER,
            SAPFLOCNUMBER,
--            NVL((SELECT MAX(SAPFLOCNUMBER) FROM MO_ELIGIBLE_PREMISES),0) + ROWNUM SAPFLOCNUMBER, -- work around for no SAP FLOC numbers
            STWCUSTOMERNUMBER,
            OWC,
            ROWID AS l_rowid
    FROM    RECEPTION.OWC_SUPPLY_POINT OSP
    WHERE OWC = p_owc
    AND NOT EXISTS (SELECT 1 FROM LU_OWC_SSW_SPIDS WHERE SSW_SPID = OSP.SPID_PK)
    ORDER BY SPID_PK, SERVICECATEGORY desc ;
             
  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM RECEPTION.OWC_SUPPLY_POINT;
             
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
 
   -- initial variables 
   
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.no_instance := 0;
   l_job.IND_STATUS := 'RUN';  
   
   -- get job no and start job
   
   P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name, 
                         l_job.no_instance, 
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
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.no_instance, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;
      
  -- start processing all records for range supplied
  FOR owc IN owc_cur
  LOOP
    -- reset counts for each OWC
    l_no_row_read_prem    := 0;
    l_no_row_dropped_prem := 0;
    l_no_row_insert_prem  := 0;
    
    l_no_row_read_cus     := 0;
    l_no_row_dropped_cus  := 0;
    l_no_row_insert_cus   := 0;
  
    l_no_row_read_sp      := 0;
    l_no_row_dropped_sp   := 0;
    l_no_row_insert_sp    := 0;
    l_no_row_update_sp    := 0;
    
    l_no_row_read_padr    := 0;
    l_no_row_dropped_padr := 0;
    l_no_row_read_cadr    := 0;
    l_no_row_dropped_cadr := 0;
    l_no_row_insert_adr   := 0;
  
    l_no_row_read_adrp    := 0;
    l_no_row_dropped_adrp := 0;
    l_no_row_insert_adrp  := 0;
  
    l_no_row_read_adrc    := 0;
    l_no_row_dropped_adrc := 0;
    l_no_row_insert_adrc  := 0;
    
    OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX, owc.OWC);
  
    l_progress := 'loop processing';
  
    LOOP
    
      FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;    
   
      FOR i IN 1..t_prop.COUNT
      LOOP
      
        l_progress := 'Process New Supply Point';
        l_err.TXT_KEY := substr(owc.OWC || ',' || t_prop(i).SPID_PK || ',' || t_prop(i).SERVICECATEGORY || ',' || t_prop(i).STWCUSTOMERNUMBER,1,30);        
        l_sp := NULL;
        l_rec_written := TRUE;
            
        IF t_prop(i).STWPROPERTYNUMBER IS NULL THEN
           l_create_prop := TRUE;
        ELSE 
           l_create_prop := FALSE;        
        END IF;

        -- keep count of all records read
        l_no_row_read := l_no_row_read + 1;

--        -- check if the SPID is a TE SPID to be excluded - if so report an exception and skip
--        l_progress := 'SELECT LU_OWC_TE_METERS';        
--        SELECT COUNT(*) 
--        INTO l_count
--        FROM LU_OWC_TE_METERS
--        WHERE OWC_SPID = t_prop(i).SPID_PK
--        AND OWC = owc.OWC
--        AND MO_RDY = 'N';   -- set flag in table to N only for Phase 2
--        
--        IF l_count > 0 THEN
--          l_rec_written := FALSE;
--          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'TE SPID provided by ' || owc.OWC,  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                     
--          l_no_row_exp := l_no_row_exp + 1;
--        END IF;
  
        -- check that imported SPID is not an STW allocated SPID -- V 0.04 
--        IF (    owc.OWC <> 'DWRCYMRU-W' 
--            AND t_prop(i).STWPROPERTYNUMBER IS NULL
--            AND t_prop(i).PAIRINGREFREASONCODE IS NULL)  
--        THEN
--           l_progress := 'SELECT LU_SPID_RANGE';        
--          SELECT COUNT(*) INTO l_count
--          FROM LU_SPID_RANGE 
--          WHERE SPID_PK = t_prop(i).SPID_PK;
--          
--          IF l_count > 0 THEN
--             l_rec_written := FALSE;
--             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'STW SPID provided by ' || owc.OWC,  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                     
--             l_no_row_exp := l_no_row_exp + 1;
--          END IF;
--        END IF;
        
        -- Get SAPFLOCNUMBER if null for existing property
        IF t_prop(i).STWPROPERTYNUMBER IS NOT NULL THEN        
           IF t_prop(i).SAPFLOCNUMBER IS NULL THEN
              l_progress := 'SELECT LU_SAP_FLOCA ';
              BEGIN
                 SELECT SAPFLOCNUMBER
                 INTO   l_floc.SAPFLOCNUMBER
                 FROM   LU_SAP_FLOCA
                 WHERE  STWPROPERTYNUMBER_PK  = t_prop(i).STWPROPERTYNUMBER;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                   l_floc.SAPFLOCNUMBER := NULL;
              END;
              t_prop(i).SAPFLOCNUMBER := l_floc.SAPFLOCNUMBER;
           END IF;
        END IF;
  
        -- Check property exists if found corespid must be same as on supplied File, if not exists create new property.
        IF t_prop(i).STWPROPERTYNUMBER IS NOT NULL THEN
           -- check property is in eligibility control table
           l_progress := 'SELECT ELIGIBILITY_CONTROL_TABLE';      
           SELECT COUNT(*)
           INTO l_count
           FROM CIS.ELIGIBILITY_CONTROL_TABLE
           WHERE NO_PROPERTY = t_prop(i).STWPROPERTYNUMBER
           AND FG_MO_RDY <> 'N';    
           
          IF l_count = 0 THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Property does not exist in ELIGIBILITY_CONTROL_TABLE',  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                        
            l_no_row_exp := l_no_row_exp + 1;
            l_no_row_dropped_prem := l_no_row_dropped_prem + 1;
          ELSE        
             l_progress := 'SELECT MO_ELIGIBLE_PREMISES';      
             BEGIN 
                SELECT CORESPID_PK 
                INTO  l_pr.CORESPID_PK
                FROM  MO_ELIGIBLE_PREMISES
                WHERE STWPROPERTYNUMBER_PK = t_prop(i).STWPROPERTYNUMBER;
             EXCEPTION
             WHEN NO_DATA_FOUND THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', 'Property supplied does NOT exist, New one being created',  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                     
                  l_no_row_war := l_no_row_war + 1;
                  l_pr.CORESPID_PK := null;
                  l_create_prop := TRUE;
             END;
          
             IF l_pr.CORESPID_PK IS NOT NULL THEN 
               IF l_pr.CORESPID_PK <> substr(t_prop(i).SPID_PK,1,10) THEN
                 l_rec_written := FALSE;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'CORESPID does not match existing on MO_ELIGIBLE_PREMISE',  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                        
                 l_no_row_exp := l_no_row_exp + 1;
                 l_no_row_dropped_prem := l_no_row_dropped_prem + 1;
               END IF;   
            END IF;
          END IF; -- if property no in ELIGIBILITY_CONTROL_TABLE
        END IF;
  
        -- Must only be sewer SPID
        IF t_prop(i).SERVICECATEGORY <> 'S' THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'OWC SPID must be sewage only',  l_err.TXT_KEY, substr(l_err.TXT_DATA ||  ',' || l_progress || ',' || t_prop(i).STWPROPERTYNUMBER, 1,100));                     
           l_no_row_exp := l_no_row_exp + 1;
        END IF;
        
        -- Write property if new
        IF (l_rec_written AND l_create_prop) THEN 
           l_no_row_read_prem := l_no_row_read_prem + 1;
           l_pr := null;
           
           IF t_prop(i).STWPROPERTYNUMBER IS NULL THEN 
             IF l_no_property = 0 THEN 
                SELECT MAX(NO_PROPERTY) + 1
                INTO   l_no_property
                FROM   CIS.TVP046PROPERTY;
             ELSE
                l_no_property := l_no_property + 1;
             END IF;
              l_pr.STWPROPERTYNUMBER_PK := l_no_property;  
           ELSE
              l_pr.STWPROPERTYNUMBER_PK := t_prop(i).STWPROPERTYNUMBER;             
           END IF;
          
           l_pr.CORESPID_PK := substr(t_prop(i).SPID_PK,1,10);
           l_pr.CUSTOMERID_PK := null;
           l_pr.SAPFLOCNUMBER := t_prop(i).SAPFLOCNUMBER;
           l_pr.RATEABLEVALUE := t_prop(i).RATEABLEVALUE;
           l_pr.OCCUPENCYSTATUS := t_prop(i).OCCUPENCYSTATUS;
           l_pr.VOABAREFERENCE := t_prop(i).VOABAREFERENCE;
           l_pr.UPRN := t_prop(i).UPRN ;
                      
           l_pr.BUILDINGWATERSTATUS := t_prop(i).BUILDINGWATERSTATUS;
           l_pr.NONPUBHEALTHRELSITE := t_prop(i).NONPUBHEALTHRELSITE;
           l_pr.NONPUBHEALTHRELSITEDSC := t_prop(i).NONPUBHEALTHRELSITEDSC;
           l_pr.PUBHEALTHRELSITEARR := t_prop(i).PUBHEALTHRELSITEARR;
           l_pr.SECTION154 := t_prop(i).SECTION154;
           
           l_pr.UPRNREASONCODE := t_prop(i).UPRNREASONCODE;         
           IF t_prop(i).UPRN IS NULL THEN
              l_pr.UPRNREASONCODE := 'OT';
           END IF;
           
           IF t_prop(i).VOABAREFERENCE IS NULL THEN
              l_pr.VOABAREFRSNCODE := 'OT';    
           ELSE
              l_pr.VOABAREFRSNCODE := NULL;
           END IF;   
           
           l_pr.PROPERTYUSECODE := 'C';        
  
           l_progress := 'INSERT MO_ELIGIBLE_PREMISES';      
           BEGIN 
              INSERT INTO MO_ELIGIBLE_PREMISES
              (STWPROPERTYNUMBER_PK, CORESPID_PK, CUSTOMERID_PK, SAPFLOCNUMBER, RATEABLEVALUE, PROPERTYUSECODE, 
              OCCUPENCYSTATUS, VOABAREFERENCE, VOABAREFRSNCODE, BUILDINGWATERSTATUS, NONPUBHEALTHRELSITE, 
              NONPUBHEALTHRELSITEDSC, PUBHEALTHRELSITEARR, SECTION154, UPRN, UPRNREASONCODE, OWC)
              VALUES
              (l_pr.STWPROPERTYNUMBER_PK, l_pr.CORESPID_PK, l_pr.CUSTOMERID_PK, l_pr.SAPFLOCNUMBER, l_pr.RATEABLEVALUE, l_pr.PROPERTYUSECODE,
              l_pr.OCCUPENCYSTATUS, l_pr.VOABAREFERENCE, l_pr.VOABAREFRSNCODE, l_pr.BUILDINGWATERSTATUS, l_pr.NONPUBHEALTHRELSITE, 
              l_pr.NONPUBHEALTHRELSITEDSC, l_pr.PUBHEALTHRELSITEARR, l_pr.SECTION154, l_pr.UPRN, l_pr.UPRNREASONCODE, owc.OWC);
           EXCEPTION 
           WHEN OTHERS THEN 
                l_rec_written := FALSE;
                l_error_number := SQLCODE;
                l_error_message := SQLERRM;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ','  || l_progress || ',' || l_pr.STWPROPERTYNUMBER_PK, 1,100));
                l_no_row_exp := l_no_row_exp + 1;
                l_no_row_dropped_prem := l_no_row_dropped_prem + 1;
           END;
               
            -- keep count of records written
            IF l_rec_written THEN
               l_no_row_insert_prem := l_no_row_insert_prem + 1;
            END IF;  
           
        END IF;     
  
        IF t_prop(i).STWPROPERTYNUMBER IS NOT NULL THEN
           l_pr.STWPROPERTYNUMBER_PK  := t_prop(i).STWPROPERTYNUMBER;   
        END IF;
  
       -- Write CUSTOMER
       IF (l_rec_written AND l_create_prop) THEN
           l_no_row_read_cus := l_no_row_read_cus + 1;
           l_cus := NULL;
           
           IF l_no_customer = 0 THEN 
              SELECT MAX(NO_LEGAL_ENTITY) + 1
              INTO   l_no_customer
              FROM   CIS.TVP036LEGALENTITY
              WHERE  NO_LEGAL_ENTITY < 999000000;
           ELSE
              l_no_customer := l_no_customer + 1;
           END IF;
  
           l_cus.CUSTOMERNUMBER_PK := l_no_customer;
           l_cus.STWPROPERTYNUMBER_PK := l_pr.STWPROPERTYNUMBER_PK;    
           l_cus.COMPANIESHOUSEREFNUM := NULL;                                 
           l_cus.CUSTOMERCLASSIFICATION := t_prop(i).CUSTOMERCLASSIFICATION;
           l_cus.CUSTOMERNAME := t_prop(i).CUSTOMERNAME;
           l_cus.CUSTOMERBANNERNAME := t_prop(i).CUSTOMERBANNERNAME;
           l_cus.STDINDUSTRYCLASSCODE := t_prop(i).STDINDUSTRYCLASSCODE;

           IF l_cus.STDINDUSTRYCLASSCODE IS NULL THEN 
              l_cus.STDINDUSTRYCLASSCODETYPE := null;
           ELSE
           l_cus.STDINDUSTRYCLASSCODETYPE := t_prop(i).STDINDUSTRYCLASSCODETYPE;
           END IF;

           l_cus.SERVICECATEGORY := NULL;
  
           l_progress := 'INSERT MO_CUSTOMER';    
           BEGIN
              INSERT INTO MO_CUSTOMER
              (CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK, COMPANIESHOUSEREFNUM, CUSTOMERCLASSIFICATION, 
               CUSTOMERNAME, CUSTOMERBANNERNAME, STDINDUSTRYCLASSCODE, STDINDUSTRYCLASSCODETYPE, SERVICECATEGORY, OWC)
              VALUES
              (l_cus.CUSTOMERNUMBER_PK, l_cus.STWPROPERTYNUMBER_PK, l_cus.COMPANIESHOUSEREFNUM, l_cus.CUSTOMERCLASSIFICATION,
               l_cus.CUSTOMERNAME, l_cus.CUSTOMERBANNERNAME, l_cus.STDINDUSTRYCLASSCODE, l_cus.STDINDUSTRYCLASSCODETYPE, l_cus.SERVICECATEGORY, owc.OWC);
           EXCEPTION
           WHEN OTHERS THEN
                l_no_row_dropped_cus := l_no_row_dropped_cus + 1;
                l_rec_written := FALSE;
                l_error_number := SQLCODE;
                l_error_message := SQLERRM;
                p_mig_batch.fn_errorlog(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.txt_key, substr(l_err.txt_data || ',' || l_progress,1,100));
                l_no_row_exp := l_no_row_exp + 1;
           END;
    
           IF l_rec_written THEN
              l_no_row_insert_cus := l_no_row_insert_cus + 1;
           END IF;
  
       END IF;     
  
       IF t_prop(i).STWCUSTOMERNUMBER IS NOT NULL THEN
          l_cus.CUSTOMERNUMBER_PK  := t_prop(i).STWCUSTOMERNUMBER;   
       END IF;
  
        -- Update existing SUPPLY POINT
        IF (not l_create_prop AND l_rec_written) THEN
           l_no_row_read_sp := l_no_row_read_sp + 1; 
           l_progress := 'SELECT MO_SUPPLY_POINT';      
           BEGIN 
              SELECT CORESPID_PK ,
                     SPID_PK,
                     PAIRINGREFREASONCODE
              INTO   l_sp.CORESPID_PK,
                     l_sp.SPID_PK,
                     l_sp.PAIRINGREFREASONCODE
              FROM   MO_SUPPLY_POINT
              WHERE  STWPROPERTYNUMBER_PK = t_prop(i).STWPROPERTYNUMBER
              AND    SERVICECATEGORY      = t_prop(i).SERVICECATEGORY ;   
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
                l_rec_written := FALSE;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Existing SPID does exist on MO_SUPPLY_POINT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
                l_no_row_exp := l_no_row_exp + 1;
                l_no_row_dropped_sp := l_no_row_dropped_sp + 1;
                l_pr.CORESPID_PK := NULL;
           END;
        
           IF (l_sp.CORESPID_PK <> substr(t_prop(i).SPID_PK,1,10) OR l_sp.SPID_PK <> t_prop(i).SPID_PK) THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'CORESPID/SPID does not match existing on MO_SUPPLY_POINT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_dropped_sp := l_no_row_dropped_sp + 1;
              l_no_row_exp := l_no_row_exp + 1;
           END IF;   
           
           IF l_rec_written THEN
              l_sp.WHOLESALERID_PK := 'SEVERN-W';
              l_sp.RETAILERID_PK := t_prop(i).RETAILERID;
              
              IF t_prop(i).OTHERWHOLESALERID IS NOT NULL THEN
                 l_sp.OTHERWHOLESALERID := owc.OWC;
              ELSE
                 IF owc.OWC = 'DWRCYMRU-W' THEN
                   l_sp.OTHERWHOLESALERID := 'SEVERN-W';           
                 ELSE             
              l_sp.OTHERWHOLESALERID := t_prop(i).OTHERWHOLESALERID;
                 END IF;
              END IF;
           
             l_sp.PAIRINGREFREASONCODE := t_prop(i).PAIRINGREFREASONCODE; 
             IF l_sp.PAIRINGREFREASONCODE = 'NOSPID' THEN
                l_sp.OTHERWHOLESALERID := NULL;
              END IF;
  
              l_progress := 'UPDATE MO_SUPPLY_POINT';            
               BEGIN
                  UPDATE MO_SUPPLY_POINT
                  SET    OTHERWHOLESALERID = l_sp.OTHERWHOLESALERID,
                         RETAILERID_PK     = l_sp.RETAILERID_PK,
                         WHOLESALERID_PK   = l_sp.WHOLESALERID_PK,
                         PAIRINGREFREASONCODE = l_sp.PAIRINGREFREASONCODE
                  WHERE  SPID_PK           = t_prop(i).SPID_PK;
               EXCEPTION
               WHEN OTHERS THEN
                    l_rec_written := FALSE;
                    l_error_number := SQLCODE;
                    l_error_message := SQLERRM;
                    p_mig_batch.fn_errorlog(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.txt_key, substr(l_err.txt_data || ',' || l_progress,1,100));
                    l_no_row_exp := l_no_row_exp + 1;
               END;
        
               IF l_rec_written THEN
                  l_no_row_update_sp := l_no_row_update_sp + 1;
               END IF;
               
           END IF;
  
        END IF;
  
       -- Write NEW SUPPLY POINT
        IF (l_create_prop AND l_rec_written) THEN
          l_no_row_read_sp := l_no_row_read_sp + 1;       
          
          l_sp := NULL;
          l_sp.REGISTRATIONSTARTDATE := NULL;
          l_sp.OTHERSERVICECATPROVIDED := 0;
          l_sp.OTHERSERVICECATPROVIDEDREASON := NULL;
          l_sp.MULTIPLEWHOLESALERFLAG := 0;
          l_sp.SPIDSTATUS := 'TRADABLE';
          l_sp.NEWCONNECTIONTYPE := NULL;
          l_sp.ACCREDITEDENTITYFLAG := 0;
          l_sp.GAPSITEALLOCATIONMETHOD := NULL;
          l_sp.OTHERSPID := NULL;             
          l_sp.LATEREGAPPLICATION := 0;
          l_SP.VOLTRANSFERFLAG := 0;
           
          l_sp.SPID_PK := t_prop(i).SPID_PK;
          l_sp.STWPROPERTYNUMBER_PK := l_pr.STWPROPERTYNUMBER_PK;
             
          l_sp.CORESPID_PK := substr(t_prop(i).SPID_PK,1,10);
          l_sp.RETAILERID_PK := t_prop(i).RETAILERID;
          l_sp.WHOLESALERID_PK := 'SEVERN-W';
          l_sp.CUSTOMERNUMBER_PK := l_cus.CUSTOMERNUMBER_PK;
          l_sp.SAPFLOCNUMBER := t_prop(i).SAPFLOCNUMBER;
          
          l_sp.SERVICECATEGORY := t_prop(i).SERVICECATEGORY;
          l_sp.SUPPLYPOINTEFFECTIVEFROMDATE := t_prop(i).SUPPLYPOINTEFFECTIVEFROMDATE;
          l_sp.DISCONRECONDEREGSTATUS := t_prop(i).DISCONRECONDEREGSTATUS;
          l_sp.LANDLORDSPID := t_prop(i).LANDLORDSPID;
          
          IF t_prop(i).OTHERWHOLESALERID IS NOT NULL THEN
             l_sp.OTHERWHOLESALERID := owc.OWC;
          ELSE
             IF owc.OWC = 'DWRCYMRU-W' THEN
               l_sp.OTHERWHOLESALERID := 'SEVERN-W';           
             ELSE             
             l_sp.OTHERWHOLESALERID := t_prop(i).OTHERWHOLESALERID;          
          END IF;
          END IF;
          
          l_sp.PAIRINGREFREASONCODE := t_prop(i).PAIRINGREFREASONCODE; 
          IF l_sp.PAIRINGREFREASONCODE = 'NOSPID' THEN
            l_sp.OTHERWHOLESALERID := NULL;
          END IF;
          
          l_sp.SUPPLYPOINTREFERENCE := substr(t_prop(i).SPID_PK,1,10); 
    
          l_progress := 'INSERT MO_SUPPLY_POINT';     
           BEGIN 
             INSERT INTO MO_SUPPLY_POINT
             (SPID_PK, STWPROPERTYNUMBER_PK, CORESPID_PK, RETAILERID_PK, WHOLESALERID_PK, CUSTOMERNUMBER_PK,
              SAPFLOCNUMBER, SERVICECATEGORY, SUPPLYPOINTEFFECTIVEFROMDATE, REGISTRATIONSTARTDATE,
              DISCONRECONDEREGSTATUS, OTHERSERVICECATPROVIDED, OTHERSERVICECATPROVIDEDREASON, MULTIPLEWHOLESALERFLAG,
              LANDLORDSPID, SPIDSTATUS, NEWCONNECTIONTYPE, ACCREDITEDENTITYFLAG, GAPSITEALLOCATIONMETHOD, OTHERSPID,
              OTHERWHOLESALERID, PAIRINGREFREASONCODE, LATEREGAPPLICATION, VOLTRANSFERFLAG, SUPPLYPOINTREFERENCE, OWC)
             VALUES
             (l_sp.SPID_PK, l_sp.STWPROPERTYNUMBER_PK, l_sp.CORESPID_PK, l_sp.RETAILERID_PK, l_sp.WHOLESALERID_PK, l_sp.CUSTOMERNUMBER_PK,
              l_sp.SAPFLOCNUMBER, l_sp.SERVICECATEGORY, l_sp.SUPPLYPOINTEFFECTIVEFROMDATE, l_sp.REGISTRATIONSTARTDATE,
              l_sp.DISCONRECONDEREGSTATUS, l_sp.OTHERSERVICECATPROVIDED, l_sp.OTHERSERVICECATPROVIDEDREASON, l_sp.MULTIPLEWHOLESALERFLAG,
              l_sp.LANDLORDSPID, l_sp.SPIDSTATUS, l_sp.NEWCONNECTIONTYPE, l_sp.ACCREDITEDENTITYFLAG, l_sp.GAPSITEALLOCATIONMETHOD, l_sp.OTHERSPID,
              l_sp.OTHERWHOLESALERID, l_sp.PAIRINGREFREASONCODE, l_sp.LATEREGAPPLICATION, l_sp.VOLTRANSFERFLAG, l_sp.SUPPLYPOINTREFERENCE, owc.OWC);
           EXCEPTION 
           WHEN DUP_VAL_ON_INDEX THEN
                l_rec_written := FALSE;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'More than 1 LE for Supply Point',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
                l_no_row_dropped_sp := l_no_row_dropped_sp + 1;
                l_no_row_exp := l_no_row_exp + 1;
           WHEN OTHERS THEN 
                l_rec_written := FALSE;
                l_error_number := SQLCODE;
                l_error_message := SQLERRM;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_dropped_sp := l_no_row_dropped_sp + 1;
                l_no_row_err := l_no_row_err + 1;
           END;
          
           IF l_rec_written THEN
              l_no_row_insert_sp := l_no_row_insert_sp + 1;
           END IF;
  
       END IF;
  
       -- Write ADDRESS of Property
       IF (l_create_prop AND l_rec_written) THEN    

          l_progress := 'Write ADDRESS of Property';   
          
          l_no_row_read_adrp := l_no_row_read_adrp + 1;
          l_no_row_read_padr := l_no_row_read_padr + 1;
          
          l_adr := NULL;
          
          IF TRIM(t_prop(i).PREMADDRESSLINE01) IS NULL THEN
             IF TRIM(t_prop(i).PREMADDRESSLINE02) IS NOT NULL THEN
                t_prop(i).PREMADDRESSLINE01        := t_prop(i).PREMADDRESSLINE02;
                t_prop(i).PREMADDRESSLINE02        := NULL;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('PREMADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_war := l_no_row_war + 1;
             ELSIF TRIM(t_prop(i).PREMADDRESSLINE03) IS NOT NULL THEN
                t_prop(i).PREMADDRESSLINE01           := t_prop(i).PREMADDRESSLINE03;
                t_prop(i).PREMADDRESSLINE03           := NULL;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('PREMADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_war := l_no_row_war + 1;
             ELSIF TRIM(t_prop(i).PREMADDRESSLINE04) IS NOT NULL THEN
                t_prop(i).PREMADDRESSLINE01           := t_prop(i).PREMADDRESSLINE04;
                t_prop(i).PREMADDRESSLINE04           := NULL;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('PREMADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_war := l_no_row_war + 1;
             ELSE
                t_prop(i).PREMADDRESSLINE01 := t_prop(i).PREMADDRESSLINE05;
                t_prop(i).PREMADDRESSLINE05 := NULL;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('PREMADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_war := l_no_row_war + 1;
             END IF;
          END IF;
          
          l_adr.UPRN := t_prop(i).UPRN;
          l_adr.UPRNREASONCODE := t_prop(i).UPRNREASONCODE;
          l_adr.PAFADDRESSKEY := t_prop(i).PREMPAFADDRESSKEY;
          l_adr.STREET2 := t_prop(i).PREMSECONDADDRESABLEOBJECT;
          l_adr.STREET3 := t_prop(i).PREMPRIMARYADDRESSABLEOBJECT; 
          l_adr.HOUSENUMBER := NULL;  -- house number (if there is one) is included in STREET
          l_adr.STREET := t_prop(i).PREMADDRESSLINE01;
          l_adr.STREET4 := t_prop(i).PREMADDRESSLINE02;
          l_adr.STREET5 := t_prop(i).PREMADDRESSLINE03;
          l_adr.DISTRICT := t_prop(i).PREMADDRESSLINE04;
          l_adr.CITY := t_prop(i).PREMADDRESSLINE05;
          l_adr.POBOX := NULL;
          
          IF FN_VALIDATE_POSTCODE(t_prop(i).PREMPOSTCODE) = 'INVALID' THEN
             l_rec_written := FALSE;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Invalid Property Postcode',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
             l_no_row_exp := l_no_row_exp + 1;
             l_no_row_dropped_padr := l_no_row_dropped_padr + 1;
          END IF;
          
          l_adr.POSTCODE := t_prop(i).PREMPOSTCODE;          
          l_adr.COUNTRY := NULL;
          l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(i).PREMLOCATIONFREETEXTDESCRIPTOR;
        
         -- Write property address details
          IF l_rec_written THEN
             l_no_cd_adr := l_no_cd_adr + 1; 
             l_adr.ADDRESS_PK := l_no_cd_adr;
  
             l_progress := 'INSERT SAP_ADDRESS';   
             BEGIN 
              INSERT INTO SAP_ADDRESS
              (ADDRESS_PK,UPRN,UPRNREASONCODE,PAFADDRESSKEY,
               STREET2,STREET3,HOUSENUMBER,
               STREET,STREET4,STREET5,DISTRICT,
               CITY,POSTCODE,COUNTRY,POBOX,
               LOCATIONFREETEXTDESCRIPTOR,CUSTOMERNUMBER_PK,STWPROPERTYNUMBER,OWC)
              VALUES
              (l_adr.ADDRESS_PK,l_adr.UPRN,l_adr.UPRNREASONCODE,l_adr.PAFADDRESSKEY,
               l_adr.STREET2,l_adr.STREET3,l_adr.HOUSENUMBER,
               l_adr.STREET,l_adr.STREET4,l_adr.STREET5,l_adr.DISTRICT,
               l_adr.CITY,l_adr.POSTCODE,l_adr.COUNTRY,l_adr.POBOX,
               l_adr.LOCATIONFREETEXTDESCRIPTOR,l_cus.CUSTOMERNUMBER_PK,NULL,owc.OWC);             
                 
                l_no_row_insert_adr := l_no_row_insert_adr + 1;
             EXCEPTION 
             WHEN OTHERS THEN 
                  l_rec_written := FALSE;
                  l_error_number := SQLCODE;
                  l_error_message := SQLERRM;
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  l_no_row_err := l_no_row_err + 1;
                  l_no_row_dropped_padr := l_no_row_dropped_padr + 1;
             END;
          END IF;
           
           -- Write PROPERTY ADDRESS
           IF l_rec_written THEN
              l_adr_pr := NULL;
              l_adr_pr.ADDRESS_PK := l_adr.ADDRESS_PK;
              l_adr_pr.STWPROPERTYNUMBER_PK := l_pr.STWPROPERTYNUMBER_PK;           
              l_adr_pr.ADDRESSUSAGEPROPERTY := 'LocatedAt'; 
              l_adr_pr.EFFECTIVEFROMDATE := SYSDATE;             
              l_adr_pr.EFFECTIVETODATE := NULL;
             
              l_progress := 'INSERT SAP_PROPERTY_ADDRESS';     
              BEGIN 
                 INSERT INTO SAP_PROPERTY_ADDRESS
                 (ADDRESSPROPERTY_PK, ADDRESS_PK, STWPROPERTYNUMBER_PK, ADDRESSUSAGEPROPERTY,
                  EFFECTIVEFROMDATE, EFFECTIVETODATE, OWC)
                 VALUES
                 (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_pr.ADDRESS_PK, l_adr_pr.STWPROPERTYNUMBER_PK, l_adr_pr.ADDRESSUSAGEPROPERTY,
                  l_adr_pr.EFFECTIVEFROMDATE, l_adr_pr.EFFECTIVETODATE, owc.OWC);
              EXCEPTION 
              WHEN OTHERS THEN
                   l_rec_written := FALSE;
                   l_error_number := SQLCODE;
                   l_error_message := SQLERRM;
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                   l_no_row_err := l_no_row_err + 1;
                   l_no_row_dropped_adrp := l_no_row_dropped_adrp + 1;
              END;
    
             -- keep count of records written
             IF l_rec_written THEN
                l_no_row_insert_adrp := l_no_row_insert_adrp + 1;
             END IF;  
          END IF;  
      
       END IF;
  
        -- Write ADDRESS OF CUSTOMER
       IF (l_rec_written AND l_create_prop ) THEN
           l_progress := 'Write ADDRESS OF CUSTOMER';   
           
           l_no_row_read_cadr := l_no_row_read_cadr + 1;
           l_no_row_read_adrc := l_no_row_read_adrc + 1;
           
           l_adr := NULL;
           l_no_cd_adr := l_no_cd_adr + 1; 
           l_adr.ADDRESS_PK := l_no_cd_adr;

           IF TRIM(t_prop(i).CUSTADDRESSLINE01) IS NULL THEN
              IF TRIM(t_prop(i).CUSTADDRESSLINE02) IS NOT NULL THEN
                 t_prop(i).CUSTADDRESSLINE01        := t_prop(i).CUSTADDRESSLINE02;
                 t_prop(i).CUSTADDRESSLINE02        := NULL;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('CUSTADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_war := l_no_row_war + 1;
              ELSIF TRIM(t_prop(i).CUSTADDRESSLINE03) IS NOT NULL THEN
                 t_prop(i).CUSTADDRESSLINE01           := t_prop(i).CUSTADDRESSLINE03;
                 t_prop(i).CUSTADDRESSLINE03           := NULL;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('CUSTADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_war := l_no_row_war + 1;
             ELSIF TRIM(t_prop(i).CUSTADDRESSLINE04) IS NOT NULL THEN
                 t_prop(i).CUSTADDRESSLINE01           := t_prop(i).CUSTADDRESSLINE04;
                 t_prop(i).CUSTADDRESSLINE04           := NULL;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('CUSTADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_war := l_no_row_war + 1;
              ELSE
                 t_prop(i).CUSTADDRESSLINE01 := t_prop(i).CUSTADDRESSLINE05;
                 t_prop(i).CUSTADDRESSLINE05 := NULL;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('CUSTADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_war := l_no_row_war + 1;
              END IF;
           END IF;
           
            l_adr.UPRN := t_prop(i).UPRN;
            l_adr.UPRNREASONCODE := t_prop(i).UPRNREASONCODE;
            l_adr.PAFADDRESSKEY := t_prop(i).CUSTPAFADDRESSKEY;
            l_adr.STREET2 := t_prop(i).CUSTSECONDADDRESABLEOBJECT;
            l_adr.STREET3 := t_prop(i).CUSTPRIMARYADDRESSABLEOBJECT; 
            l_adr.HOUSENUMBER := NULL;  -- house number (if there is one) is included in STREET
            l_adr.STREET := t_prop(i).CUSTADDRESSLINE01;
            l_adr.STREET4 := t_prop(i).CUSTADDRESSLINE02;
            l_adr.STREET5 := t_prop(i).CUSTADDRESSLINE03;
            l_adr.DISTRICT := t_prop(i).CUSTADDRESSLINE04;
            l_adr.CITY := t_prop(i).CUSTADDRESSLINE05;
            l_adr.POBOX := NULL;           
  
            IF INSTR(t_prop(i).CUSTPOSTCODE, ' ') = 0 THEN 
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Pre Customer postcode check is invalid',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
               l_no_row_exp := l_no_row_exp + 1;
               l_no_row_dropped_cadr := l_no_row_dropped_cadr + 1;
               l_rec_written := FALSE;
            ELSE 
             IF FN_VALIDATE_POSTCODE(t_prop(i).CUSTPOSTCODE) = 'INVALID' THEN
                l_rec_written := FALSE;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Invalid Customer Postcode',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
                l_no_row_exp := l_no_row_exp + 1;
                l_no_row_dropped_cadr := l_no_row_dropped_cadr + 1;
             END IF;
           END IF;
                  
           l_adr.POSTCODE := t_prop(i).CUSTPOSTCODE;
           l_adr.COUNTRY := t_prop(i).CUSTCOUNTRY;
           l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(i).CUSTLOCATIONFREETEXTDESCRIPTOR;
  
           IF l_rec_written THEN        
               l_progress := 'INSERT SAP_ADDRESS';   
               BEGIN 
                  INSERT INTO SAP_ADDRESS
                  (ADDRESS_PK,UPRN,UPRNREASONCODE,PAFADDRESSKEY,
                   STREET2,STREET3,HOUSENUMBER,
                   STREET,STREET4,STREET5,DISTRICT,
                   CITY,POSTCODE,COUNTRY,POBOX,
                   LOCATIONFREETEXTDESCRIPTOR,CUSTOMERNUMBER_PK,STWPROPERTYNUMBER,OWC)
                  VALUES
                  (l_adr.ADDRESS_PK,l_adr.UPRN,l_adr.UPRNREASONCODE,l_adr.PAFADDRESSKEY,
                   l_adr.STREET2,l_adr.STREET3,l_adr.HOUSENUMBER,
                   l_adr.STREET,l_adr.STREET4,l_adr.STREET5,l_adr.DISTRICT,
                   l_adr.CITY,l_adr.POSTCODE,l_adr.COUNTRY,l_adr.POBOX,
                   l_adr.LOCATIONFREETEXTDESCRIPTOR,l_cus.CUSTOMERNUMBER_PK,NULL,owc.OWC);     
                       
                   l_no_row_insert_adr := l_no_row_insert_adr + 1;
               EXCEPTION 
               WHEN OTHERS THEN 
                    l_rec_written := FALSE;
                    l_error_number := SQLCODE;
                    l_error_message := SQLERRM;
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                    l_no_row_err := l_no_row_err + 1;
                    l_no_row_dropped_cadr := l_no_row_dropped_cadr + 1;
               END;
          END IF;
          
          -- Write ADDRESS OF CUSTOMER    
          
          IF l_rec_written THEN     
             l_adr_cus.ADDRESS_PK := l_adr.ADDRESS_PK;
             l_adr_cus.STWPROPERTYNUMBER_PK := l_pr.STWPROPERTYNUMBER_PK;           
             l_adr_cus.CUSTOMERNUMBER_PK := l_cus.CUSTOMERNUMBER_PK;   
             l_adr_cus.ADDRESSUSAGEPROPERTY := 'BilledAt'; 
             l_adr_cus.EFFECTIVEFROMDATE := SYSDATE;             
             l_adr_cus.EFFECTIVETODATE := null;    
  
             l_progress := 'INSERT SAP_CUST_ADDRESS';     
    
             BEGIN 
               INSERT INTO SAP_CUST_ADDRESS
               (ADDRESSPROPERTY_PK, ADDRESS_PK, CUSTOMERNUMBER_PK, ADDRESSUSAGEPROPERTY, 
                EFFECTIVEFROMDATE, EFFECTIVETODATE, STWPROPERTYNUMBER_PK, OWC)
               VALUES
               (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_cus.ADDRESS_PK, l_adr_cus.CUSTOMERNUMBER_PK, l_adr_cus.ADDRESSUSAGEPROPERTY,
                l_adr_cus.EFFECTIVEFROMDATE, l_adr_cus.EFFECTIVETODATE, l_adr_cus.STWPROPERTYNUMBER_PK, owc.OWC);
             EXCEPTION 
             WHEN OTHERS THEN 
                 l_rec_written := FALSE;
                 l_error_number := SQLCODE;
                 l_error_message := SQLERRM;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_err := l_no_row_err + 1;
                 l_no_row_dropped_adrc := l_no_row_dropped_adrc + 1;
             END;
    
             -- keep count of records written
             IF l_rec_written THEN
                l_no_row_insert_adrc := l_no_row_insert_adrc + 1;
             END IF;  
          END IF;  
      
        END IF; 
  
        IF l_rec_written = FALSE THEN
           l_no_row_dropped := l_no_row_dropped + 1;
        ELSE
           l_no_row_insert := l_no_row_insert + 1;
        END IF;         
  
        -- if tolearance limit has een exceeded, set error message and exit out
        IF (   l_no_row_exp > l_job.EXP_TOLERANCE
            OR l_no_row_err > l_job.ERR_TOLERANCE
            OR l_no_row_war > l_job.WAR_TOLERANCE)   
        THEN
           CLOSE cur_prop; 
           l_job.IND_STATUS := 'ERR';
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
           P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.no_instance, l_job.IND_STATUS);
           COMMIT;
           return_code := -1;
           RETURN; 
        END IF;
        
      END LOOP;
  
      IF t_prop.COUNT < l_job.NO_COMMIT THEN
         EXIT;
      ELSE
         COMMIT;
      END IF;
       
    END LOOP;
    
    CLOSE cur_prop;  

    -- write OWC specific counts 
    l_progress := 'Writing OWC counts ' || owc.OWC;  
    
    -- Premises
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_ELIGIBLE_PREMISES');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_prem, owc.OWC || ' Distinct Eligible Properties read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_prem, owc.OWC || ' Eligible Properties dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_prem, owc.OWC || ' Eligible Properties written to MO_ELIGIBLE_PREMISE during Transform'); 
    
    l_tot_row_read_prem := l_tot_row_read_prem + l_no_row_read_prem;
    l_tot_row_dropped_prem := l_tot_row_dropped_prem + l_no_row_dropped_prem;
    l_tot_row_insert_prem := l_tot_row_insert_prem + l_no_row_insert_prem;
    
    -- Supply Point
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_SUPPLY_POINT');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_sp, owc.OWC || ' Distinct Supply Points read in to transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_sp, owc.OWC || ' Distinct Supply Points dropped during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_sp,  owc.OWC || ' Distinct Supply Points written MO_SUPPLY_POINT during Transform');
  
    l_tot_row_read_sp := l_tot_row_read_sp + l_no_row_read_sp;
    l_tot_row_dropped_sp := l_tot_row_dropped_sp + l_no_row_dropped_sp;
    l_tot_row_insert_sp := l_tot_row_insert_sp + l_no_row_insert_sp;
    
    -- Customer
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_CUSTOMER');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_cus,    owc.OWC || ' Distinct Eligible Customers read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_cus, owc.OWC || ' Eligible Customers  dropped during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_cus,  owc.OWC || ' Eligible Customers written to MO_CUSTOMER during Transform');
    
    l_tot_row_read_cus := l_tot_row_read_cus + l_no_row_read_cus;
    l_tot_row_dropped_cus := l_tot_row_dropped_cus + l_no_row_dropped_cus;
    l_tot_row_insert_cus := l_tot_row_insert_cus + l_no_row_insert_cus;
    
    -- Address
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_ADDRESS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_padr + l_no_row_read_cadr, owc.OWC || ' Distinct addresses read');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_padr + l_no_row_dropped_cadr, owc.OWC || ' Distinct addresses dropped');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_adr, owc.OWC || ' Distinct addresses inserted to MO_ADDRESS during Transform');
  
    l_tot_row_read_adr := l_tot_row_read_adr + l_no_row_read_padr + l_no_row_read_cadr;
    l_tot_row_dropped_adr := l_tot_row_dropped_adr + l_no_row_dropped_padr + l_no_row_dropped_cadr;
    l_tot_row_insert_adr := l_tot_row_insert_adr + l_no_row_insert_adr;  
  
    -- Property Address
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_PROPERTY_ADDRESS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE,l_no_row_read_adrp, owc.OWC || ' Distinct property addresses read');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE,l_no_row_dropped_adrp, owc.OWC || ' Distinct property addresses dropped');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE,l_no_row_insert_adrp, owc.OWC || ' Distinct property addresses inserted to MO_PROPERTY_ADDRESS during Transform');
  
    l_tot_row_read_adrp := l_tot_row_read_adrp + l_no_row_read_adrp;
    l_tot_row_dropped_adrp := l_tot_row_dropped_adrp + l_no_row_dropped_adrp;
    l_tot_row_insert_adrp := l_tot_row_insert_adrp + l_no_row_insert_adrp;  
  
    -- Customer Address
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_CUSTOMER_ADDRESS');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_adrc, owc.OWC || ' Distinct customer addresses read');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_adrc, owc.OWC || ' Distinct customer addresses dropped');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_adrc, owc.OWC || ' Distinct customer addresses inserted to MO_CUSTOMER_ADDRESS during Transform');
  
    l_tot_row_read_adrc := l_tot_row_read_adrc + l_no_row_read_adrc;
    l_tot_row_dropped_adrc := l_tot_row_dropped_adrc + l_no_row_dropped_adrc;
    l_tot_row_insert_adrc := l_tot_row_insert_adrc + l_no_row_insert_adrc;  

  END LOOP; -- owc_cur

  -- write total counts
  l_progress := 'Writing total counts';  

  -- Premises
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP26', 910, l_tot_row_read_prem, 'OWC Distinct Eligible Properties read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP26', 920, l_tot_row_dropped_prem, 'OWC Eligible Properties dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP26', 930, l_tot_row_insert_prem, 'OWC Eligible Properties written to MO_ELIGIBLE_PREMISE during Transform');    
  
  -- Supply Point
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1000, l_tot_row_read_sp, 'OWC Distinct Supply Points read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1010, l_tot_row_dropped_sp, 'OWC Distinct Supply Points dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1020, l_tot_row_insert_sp,  'OWC Distinct Supply Points Written to Table');

  -- Customer
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP27', 940, l_tot_row_read_cus,    'OWC Distinct Eligible Customers read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP27', 950, l_tot_row_dropped_cus, 'OWC Eligible Customers  dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP27', 960, l_tot_row_insert_cus,  'OWC Eligible Customers written to MO_CUSTOMER during Transform');

  -- Address
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 970, l_tot_row_read_adr, 'OWC Distinct addresses read');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 980, l_tot_row_dropped_adr, 'OWC Distinct addresses dropped');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 990, l_tot_row_insert_adr, 'OWC Distinct addresses inserted');

  -- Property Address
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 971,l_tot_row_read_adrp, 'OWC Distinct property addresses read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 981,l_tot_row_dropped_adrp, 'OWC Distinct property addresses  dropped');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 991,l_tot_row_insert_adrp, 'OWC Distinct property addresses  inserted');

  -- Customer Address
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 973, l_tot_row_read_adrc, 'OWC Distinct customer addresses read');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 983, l_tot_row_dropped_adrc, 'OWC Distinct customer addresses dropped');
  P_MIG_BATCH.FN_RECONLOg(no_batch, l_job.no_instance, 'CP28', 993, l_tot_row_insert_adrc, 'OWC Distinct customer addresses inserted');
  
  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.no_instance, l_job.IND_STATUS);  
     
  l_progress := 'End';  
  
  COMMIT;  
   
EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));     
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.no_instance, l_job.IND_STATUS);
     return_code := -1;
END P_OWC_SAP_SUPPLY_POINT;
/
show errors;

exit;