CREATE OR REPLACE PROCEDURE P_FIN_TRAN_SUPPLY_POINT(no_batch          IN MIG_BATCHSTATUS.no_batch%TYPE,
                                                    no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                    return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: FIN Supply Point
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_FIN_TRAN_SUPPLY_POINT.sql
--
-- Subversion $Revision: 5391 $
--
-- CREATED        : 27/07/2016
--
-- DESCRIPTION    : Procedure to populate MO_ELIGIBLE_PREMISES, MO_CUSTOMER, MO_CUSTOMER_ADDRESS,
--                  MO_SUPPLY_POINT from SAP and OWC supplied data - OWC_SUPPLY_POINT and
--                  SAP_SUPPLY_POINT.
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      27/07/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_FIN_TRAN_SUPPLY_POINT';
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
  l_adr                         MO_ADDRESS%ROWTYPE;  
  l_adr_pr                      MO_PROPERTY_ADDRESS%ROWTYPE := NULL;  
  l_adr_cus                     Mo_Cust_Address%Rowtype := Null;  
  l_no_row_read_prop            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped_prop         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_cross                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_insert_prop          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;  
  l_no_row_insert_sp            mig_cplog.recon_measure_total%type := 0;
  l_no_row_read_cus             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_cus           mig_cplog.recon_measure_total%type := 0;
  l_no_row_dropped_cus          mig_cplog.recon_measure_total%type := 0;
  l_no_row_read_adrp            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_adrp          MIG_CPLOG.RECON_MEASURE_TOTAL%type := 0;
  l_no_row_read_adrc            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_adrc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_cusadr        MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_insert_propadr       MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE := 0;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE := 0;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE := 0;
  l_rec_written                 BOOLEAN;
  l_cross_border                BOOLEAN;
  l_no_cd_adr                   NUMBER(9) := 0;
   
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
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
            STWCUSTOMERNUMBER,
            OWC
    FROM    RECEPTION.SAP_SUPPLY_POINT
    WHERE   STWPROPERTYNUMBER BETWEEN p_no_property_start AND p_no_property_end -- and STWPROPERTYNUMBER = 547044824 --and rownum < 1001
    ORDER BY STWPROPERTYNUMBER, SERVICECATEGORY desc ;
             
             
TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
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
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing';

  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;    
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop(I).STWPROPERTYNUMBER || ',' || t_prop(I).SERVICECATEGORY || ',' || t_prop(I).STWCUSTOMERNUMBER;
      l_sp := NULL;
      l_rec_written := TRUE;

      -- keep count of all records read
      l_no_row_read := l_no_row_read + 1;

       -- if water company Severn Trent must exist on lookup file.
      IF t_prop(I).OWC = 'SEVERN' THEN 
         l_progress := 'SELECT LU_SPID_RANGE';      
         BEGIN 
            SELECT SPID_PK 
            INTO  l_lu.SPID_PK
            FROM  LU_SPID_RANGE
            WHERE SPID_PK = t_prop(I).SPID_PK;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Severn Trent SPID not in SPID range',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_exp := l_no_row_exp + 1;
         END;
         l_cross_border := FALSE;
      ELSE
         l_cross_border := TRUE;
      END IF;

--      l_progress := 'CHECK SAPFLOCNUMBER';   
--      IF t_prop(I).SAPFLOCNUMBER IS NULL THEN
--         l_rec_written := FALSE;
--         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'SAPFLOCNUMBER IS NULL',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
--         l_no_row_exp := l_no_row_exp + 1;
--      END IF;
      
      -- Write property upon change of property
      
      IF (     l_prev_prp  <> t_prop(I).STWPROPERTYNUMBER
          AND  l_rec_written                             )
      THEN 
         l_no_row_read_prop := l_no_row_read_prop + 1;
         l_pr := null;
         l_pr.STWPROPERTYNUMBER_PK := t_prop(I).STWPROPERTYNUMBER;
         l_pr.CORESPID_PK := substr(t_prop(I).SPID_PK,1,10);
         l_pr.CUSTOMERID_PK := null;
         l_pr.SAPFLOCNUMBER := t_prop(I).SAPFLOCNUMBER;
         l_pr.RATEABLEVALUE := t_prop(I).RATEABLEVALUE;
         l_pr.OCCUPENCYSTATUS := t_prop(I).OCCUPENCYSTATUS;
         l_pr.VOABAREFERENCE := t_prop(I).VOABAREFERENCE;
         l_pr.VOABAREFRSNCODE := t_prop(I).VOABAREFRSNCODE;
         l_pr.BUILDINGWATERSTATUS := t_prop(I).BUILDINGWATERSTATUS;
         l_pr.NONPUBHEALTHRELSITE := t_prop(I).NONPUBHEALTHRELSITE;
         l_pr.NONPUBHEALTHRELSITEDSC := t_prop(I).NONPUBHEALTHRELSITEDSC;
         l_pr.PUBHEALTHRELSITEARR := t_prop(I).PUBHEALTHRELSITEARR;
         l_pr.SECTION154 := t_prop(I).SECTION154;
         l_pr.UPRN := t_prop(I).UPRN;
         l_pr.UPRNREASONCODE := t_prop(I).UPRNREASONCODE;         
         l_pr.PROPERTYUSECODE := null;

         l_progress := 'INSERT MO_ELIGIBLE_PREMISES';      
         BEGIN 
            INSERT INTO MO_ELIGIBLE_PREMISES
            (STWPROPERTYNUMBER_PK, CORESPID_PK, CUSTOMERID_PK, SAPFLOCNUMBER, RATEABLEVALUE, PROPERTYUSECODE, 
            OCCUPENCYSTATUS, VOABAREFERENCE, VOABAREFRSNCODE, BUILDINGWATERSTATUS, NONPUBHEALTHRELSITE, 
            NONPUBHEALTHRELSITEDSC, PUBHEALTHRELSITEARR, SECTION154, UPRN, UPRNREASONCODE)
            VALUES
            (l_pr.STWPROPERTYNUMBER_PK, l_pr.CORESPID_PK, l_pr.CUSTOMERID_PK, l_pr.SAPFLOCNUMBER, l_pr.RATEABLEVALUE, l_pr.PROPERTYUSECODE,
            l_pr.OCCUPENCYSTATUS, l_pr.VOABAREFERENCE, l_pr.VOABAREFRSNCODE, l_pr.BUILDINGWATERSTATUS, l_pr.NONPUBHEALTHRELSITE, 
            l_pr.NONPUBHEALTHRELSITEDSC, l_pr.PUBHEALTHRELSITEARR, l_pr.SECTION154, l_pr.UPRN, l_pr.UPRNREASONCODE);
         EXCEPTION 
         WHEN OTHERS THEN 
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_exp := l_no_row_exp + 1;
              l_no_row_dropped_prop := l_no_row_dropped_prop + 1;
         END;
        
          -- keep count of records written
          IF l_rec_written THEN
             l_no_row_insert_prop := l_no_row_insert_prop + 1;
          END IF;  
         
      END IF;     --- change of property

      -- check that if premise has been created, may not be if Water spid was invalid 
      
      IF t_prop(I).OWC <> 'SEVERN' THEN 
         l_progress := 'SELECT MO_ELIGIBLE_PREMISES';      
         BEGIN 
            SELECT CORESPID_PK 
            INTO   l_pr.CORESPID_PK
            FROM   MO_ELIGIBLE_PREMISES
            WHERE  CORESPID_PK = substr(t_prop(I).SPID_PK,1,10);
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Water SPID not created',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_exp := l_no_row_exp + 1;
         END;
      END IF;

     -- Write SUPPLY POINT

     IF l_rec_written THEN
        l_sp := null;
        l_sp.REGISTRATIONSTARTDATE := NULL;
        l_sp.OTHERSERVICECATPROVIDED := 0;
        l_sp.OTHERSERVICECATPROVIDEDREASON := null;
        l_sp.MULTIPLEWHOLESALERFLAG := 0;
        l_sp.SPIDSTATUS := 'TRADABLE';
        l_sp.NEWCONNECTIONTYPE := NULL;
        l_sp.ACCREDITEDENTITYFLAG := 0;
        l_sp.GAPSITEALLOCATIONMETHOD := NULL;
        l_sp.OTHERSPID := NULL;             
        l_sp.LATEREGAPPLICATION := 0;
        l_SP.VOLTRANSFERFLAG := 0;
         
        l_sp.SPID_PK := t_prop(I).SPID_PK;
        l_sp.STWPROPERTYNUMBER_PK := t_prop(I).STWPROPERTYNUMBER;
        l_sp.CORESPID_PK := substr(t_prop(I).SPID_PK,1,10);
        l_sp.RETAILERID_PK := t_prop(I).RETAILERID;
        l_sp.WHOLESALERID_PK := t_prop(I).WHOLESALERID;
        l_sp.CUSTOMERNUMBER_PK := t_prop(I).STWCUSTOMERNUMBER;
        l_sp.SAPFLOCNUMBER := t_prop(I).SAPFLOCNUMBER;
        
        l_sp.SERVICECATEGORY := t_prop(I).SERVICECATEGORY;
        l_sp.SUPPLYPOINTEFFECTIVEFROMDATE := t_prop(I).SUPPLYPOINTEFFECTIVEFROMDATE;
        l_sp.DISCONRECONDEREGSTATUS := t_prop(I).DISCONRECONDEREGSTATUS;
        l_sp.LANDLORDSPID := t_prop(I).LANDLORDSPID;
        l_sp.OTHERWHOLESALERID := t_prop(I).OTHERWHOLESALERID;
        
        l_sp.PAIRINGREFREASONCODE := t_prop(I).PAIRINGREFREASONCODE; 
        l_sp.SUPPLYPOINTREFERENCE := substr(t_prop(I).SPID_PK,1,10); 
  
        l_progress := 'INSERT MO_SUPPLY_POINT';     
         BEGIN 
           INSERT INTO MO_SUPPLY_POINT
           (SPID_PK, STWPROPERTYNUMBER_PK, CORESPID_PK, RETAILERID_PK, WHOLESALERID_PK, CUSTOMERNUMBER_PK,
            SAPFLOCNUMBER, SERVICECATEGORY, SUPPLYPOINTEFFECTIVEFROMDATE, REGISTRATIONSTARTDATE,
            DISCONRECONDEREGSTATUS, OTHERSERVICECATPROVIDED, OTHERSERVICECATPROVIDEDREASON, MULTIPLEWHOLESALERFLAG,
            LANDLORDSPID, SPIDSTATUS, NEWCONNECTIONTYPE, ACCREDITEDENTITYFLAG, GAPSITEALLOCATIONMETHOD, OTHERSPID,
            OTHERWHOLESALERID, PAIRINGREFREASONCODE, LATEREGAPPLICATION, VOLTRANSFERFLAG, SUPPLYPOINTREFERENCE)
           VALUES
           (l_sp.SPID_PK, l_sp.STWPROPERTYNUMBER_PK, l_sp.CORESPID_PK, l_sp.RETAILERID_PK, l_sp.WHOLESALERID_PK, l_sp.CUSTOMERNUMBER_PK,
            l_sp.SAPFLOCNUMBER, l_sp.SERVICECATEGORY, l_sp.SUPPLYPOINTEFFECTIVEFROMDATE, l_sp.REGISTRATIONSTARTDATE,
            l_sp.DISCONRECONDEREGSTATUS, l_sp.OTHERSERVICECATPROVIDED, l_sp.OTHERSERVICECATPROVIDEDREASON, l_sp.MULTIPLEWHOLESALERFLAG,
            l_sp.LANDLORDSPID, l_sp.SPIDSTATUS, l_sp.NEWCONNECTIONTYPE, l_sp.ACCREDITEDENTITYFLAG, l_sp.GAPSITEALLOCATIONMETHOD, l_sp.OTHERSPID,
            l_sp.OTHERWHOLESALERID, l_sp.PAIRINGREFREASONCODE, l_sp.LATEREGAPPLICATION, l_sp.VOLTRANSFERFLAG, l_sp.SUPPLYPOINTREFERENCE);
         EXCEPTION 
         WHEN DUP_VAL_ON_INDEX THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'More than 1 LE for Supply Point',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
              l_no_row_exp := l_no_row_exp + 1;
         WHEN OTHERS THEN 
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_err := l_no_row_err + 1;
         END;
        
         IF l_rec_written THEN
            l_no_row_insert_sp := l_no_row_insert_sp + 1;
            IF l_cross_border THEN
               l_no_row_cross := l_no_row_cross + 1;
            END IF;
         END IF;

     END IF;
 
     -- Write CUSTOMER

     IF l_rec_written THEN      
        IF (   l_prev_cus <> t_prop(I).STWCUSTOMERNUMBER
            OR l_prev_prp <> t_prop(I).STWPROPERTYNUMBER)
        then
           l_no_row_read_cus := l_no_row_read_cus + 1;
           l_cus := NULL;
           l_cus.CUSTOMERNUMBER_PK := t_prop(I).STWCUSTOMERNUMBER;
           l_cus.STWPROPERTYNUMBER_PK := t_prop(I).STWPROPERTYNUMBER;    
           l_cus.COMPANIESHOUSEREFNUM := NULL;                                 
           l_cus.CUSTOMERCLASSIFICATION := t_prop(I).CUSTOMERCLASSIFICATION;
           l_cus.CUSTOMERNAME := t_prop(I).CUSTOMERNAME;
           l_cus.CUSTOMERBANNERNAME := t_prop(I).CUSTOMERBANNERNAME;
           l_cus.STDINDUSTRYCLASSCODE := t_prop(I).STDINDUSTRYCLASSCODE;
           l_cus.STDINDUSTRYCLASSCODETYPE := t_prop(I).STDINDUSTRYCLASSCODETYPE;
           l_cus.SERVICECATEGORY := NULL;

           l_progress := 'INSERT MO_CUSTOMER';    
           BEGIN
              INSERT INTO MO_CUSTOMER
              (CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK, COMPANIESHOUSEREFNUM, CUSTOMERCLASSIFICATION, 
               CUSTOMERNAME, CUSTOMERBANNERNAME, STDINDUSTRYCLASSCODE, STDINDUSTRYCLASSCODETYPE, SERVICECATEGORY)
              VALUES
              (l_cus.CUSTOMERNUMBER_PK, l_cus.STWPROPERTYNUMBER_PK, l_cus.COMPANIESHOUSEREFNUM, l_cus.CUSTOMERCLASSIFICATION,
               l_cus.CUSTOMERNAME, l_cus.CUSTOMERBANNERNAME, l_cus.STDINDUSTRYCLASSCODE, l_cus.STDINDUSTRYCLASSCODETYPE, l_cus.SERVICECATEGORY);
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
          
     END IF;     
        
     -- Write ADDRESS

      l_adr := NULL;
      l_adr.UPRN := t_prop(I).UPRN;      
      l_adr.PAFADDRESSKEY := t_prop(I).PREMPAFADDRESSKEY;
      l_adr.PROPERTYNUMBERPROPERTY := NULL;
      l_adr.CUSTOMERNUMBERPROPERTY := NULL;
      l_adr.UPRNREASONCODE := t_prop(I).UPRNREASONCODE;
      l_adr.SECONDADDRESABLEOBJECT := t_prop(I).PREMSECONDADDRESABLEOBJECT;
      l_adr.PRIMARYADDRESSABLEOBJECT := t_prop(I).PREMPRIMARYADDRESSABLEOBJECT;      
      l_adr.ADDRESSLINE01 := t_prop(I).PREMADDRESSLINE01;
      l_adr.ADDRESSLINE02 := t_prop(I).PREMADDRESSLINE02;
      l_adr.ADDRESSLINE03 := t_prop(I).PREMADDRESSLINE03;
      l_adr.ADDRESSLINE04 := t_prop(I).PREMADDRESSLINE04;
      l_adr.ADDRESSLINE05 := t_prop(I).PREMADDRESSLINE05;

      IF TRIM(l_adr.ADDRESSLINE01) IS NULL THEN
         IF TRIM(l_adr.ADDRESSLINE02) IS NOT NULL THEN
            l_adr.ADDRESSLINE01        := l_adr.ADDRESSLINE02;
            l_adr.ADDRESSLINE02        := NULL;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_war := l_no_row_war + 1;
         ELSIF TRIM(l_adr.ADDRESSLINE03) IS NOT NULL THEN
            l_adr.ADDRESSLINE01           := l_adr.ADDRESSLINE03;
            l_adr.ADDRESSLINE03           := NULL;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_war := l_no_row_war + 1;
         ELSIF TRIM(l_adr.ADDRESSLINE04) IS NOT NULL THEN
            l_adr.ADDRESSLINE01           := l_adr.ADDRESSLINE04;
            l_adr.ADDRESSLINE04           := NULL;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_war := l_no_row_war + 1;
         ELSE
            l_adr.ADDRESSLINE01 := l_adr.ADDRESSLINE05;
            l_adr.ADDRESSLINE05 := NULL;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_war := l_no_row_war + 1;
         END IF;
      END IF;
      
      IF FN_VALIDATE_POSTCODE(t_prop(I).PREMPOSTCODE) = 'INVALID' THEN
         l_rec_written := FALSE;
         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Invalid Postcode',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
         l_no_row_exp := l_no_row_exp + 1;
      END IF;
      
      l_adr.POSTCODE := t_prop(I).PREMPOSTCODE;          
      l_adr.COUNTRY := null;
      l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(I).PREMLOCATIONFREETEXTDESCRIPTOR;
      l_adr.FOREIGN_ADDRESS := 'N';   -----******* FIX  ???
      
      
      -- Write property address details

      IF (    l_prev_prp  <> t_prop(I).STWPROPERTYNUMBER
          AND l_rec_written                             )
      THEN   
         l_no_row_read_adrp := l_no_row_read_adrp + 1; 
         l_no_cd_adr := l_no_cd_adr + 1; 
         l_adr.ADDRESS_PK := l_no_cd_adr;
          
         l_progress := 'INSERT MO_ADDRESS';   
         BEGIN 
            INSERT INTO MO_ADDRESS
            (ADDRESS_PK, UPRN, PAFADDRESSKEY, PROPERTYNUMBERPROPERTY, CUSTOMERNUMBERPROPERTY, 
             UPRNREASONCODE, SECONDADDRESABLEOBJECT, PRIMARYADDRESSABLEOBJECT, ADDRESSLINE01, 
             ADDRESSLINE02, ADDRESSLINE03, ADDRESSLINE04, ADDRESSLINE05, POSTCODE,
             COUNTRY, LOCATIONFREETEXTDESCRIPTOR, FOREIGN_ADDRESS)
            VALUES
            (l_adr.ADDRESS_PK, l_adr.UPRN, l_adr.PAFADDRESSKEY, l_adr.PROPERTYNUMBERPROPERTY, l_adr.CUSTOMERNUMBERPROPERTY, 
             l_adr.UPRNREASONCODE, l_adr.SECONDADDRESABLEOBJECT, l_adr.PRIMARYADDRESSABLEOBJECT, l_adr.ADDRESSLINE01, 
             l_adr.ADDRESSLINE02, l_adr.ADDRESSLINE03, l_adr.ADDRESSLINE04, l_adr.ADDRESSLINE05, l_adr.POSTCODE,
             l_adr.COUNTRY, l_adr.LOCATIONFREETEXTDESCRIPTOR, l_adr.FOREIGN_ADDRESS);
         EXCEPTION 
         WHEN OTHERS THEN 
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_err := l_no_row_err + 1;
         END;

         -- Write PROPERTY ADDRESS
         IF l_rec_written THEN
            l_no_row_insert_adrp := l_no_row_insert_adrp + 1;
            l_adr_pr := NULL;
            l_adr_pr.ADDRESS_PK := l_adr.ADDRESS_PK;
            l_adr_pr.STWPROPERTYNUMBER_PK := t_prop(I).STWPROPERTYNUMBER;           
            l_adr_pr.ADDRESSUSAGEPROPERTY := 'LocatedAt'; 
            l_adr_pr.EFFECTIVEFROMDATE := SYSDATE;             
            l_adr_pr.EFFECTIVETODATE := NULL;
           
            l_progress := 'INSERT MO_PROPERTY_ADDRESS';     
            BEGIN 
               INSERT INTO MO_PROPERTY_ADDRESS 
               (ADDRESSPROPERTY_PK, ADDRESS_PK, STWPROPERTYNUMBER_PK, ADDRESSUSAGEPROPERTY,
                EFFECTIVEFROMDATE, EFFECTIVETODATE)
               VALUES
               (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_pr.ADDRESS_PK, l_adr_pr.STWPROPERTYNUMBER_PK, l_adr_pr.ADDRESSUSAGEPROPERTY,
                l_adr_pr.EFFECTIVEFROMDATE, l_adr_pr.EFFECTIVETODATE);
            EXCEPTION 
            WHEN OTHERS THEN
                 l_rec_written := FALSE;
                 l_error_number := SQLCODE;
                 l_error_message := SQLERRM;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_err := l_no_row_err + 1;
            END;
  
           -- keep count of records written
           IF l_rec_written THEN
              l_no_row_insert_propadr := l_no_row_insert_propadr + 1;
           END IF;  
        END IF;  
           
      END IF;

      -- Write ADDRESS OF CUSTOMER
      IF (     l_rec_written   
          AND (   l_prev_prp  <> t_prop(I).STWPROPERTYNUMBER
               OR l_prev_cus  <> t_prop(I).STWCUSTOMERNUMBER ) )
      THEN
         l_no_row_read_adrc := l_no_row_read_adrc + 1;
         l_adr := NULL;
         l_no_cd_adr := l_no_cd_adr + 1; 
         l_adr.ADDRESS_PK := l_no_cd_adr;
         l_adr.UPRN := t_prop(I).UPRN;      
         l_adr.PAFADDRESSKEY := t_prop(I).CUSTPAFADDRESSKEY;
         l_adr.PROPERTYNUMBERPROPERTY := NULL;
         l_adr.CUSTOMERNUMBERPROPERTY := NULL;
         l_adr.UPRNREASONCODE := t_prop(I).UPRNREASONCODE;
         l_adr.SECONDADDRESABLEOBJECT := t_prop(I).CUSTSECONDADDRESABLEOBJECT;
         l_adr.PRIMARYADDRESSABLEOBJECT := t_prop(I).CUSTPRIMARYADDRESSABLEOBJECT;      
         l_adr.ADDRESSLINE01 := t_prop(I).CUSTADDRESSLINE01;
         l_adr.ADDRESSLINE02 := t_prop(I).CUSTADDRESSLINE02;
         l_adr.ADDRESSLINE03 := t_prop(I).CUSTADDRESSLINE03;
         l_adr.ADDRESSLINE04 := t_prop(I).CUSTADDRESSLINE04;
         l_adr.ADDRESSLINE05 := t_prop(I).CUSTADDRESSLINE05;

         IF TRIM(l_adr.ADDRESSLINE01) IS NULL THEN
            IF TRIM(l_adr.ADDRESSLINE02) IS NOT NULL THEN
               l_adr.ADDRESSLINE01        := l_adr.ADDRESSLINE02;
               l_adr.ADDRESSLINE02        := NULL;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_war := l_no_row_war + 1;
            ELSIF TRIM(l_adr.ADDRESSLINE03) IS NOT NULL THEN
               l_adr.ADDRESSLINE01           := l_adr.ADDRESSLINE03;
               l_adr.ADDRESSLINE03           := NULL;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_war := l_no_row_war + 1;
           ELSIF TRIM(l_adr.ADDRESSLINE04) IS NOT NULL THEN
               l_adr.ADDRESSLINE01           := l_adr.ADDRESSLINE04;
               l_adr.ADDRESSLINE04           := NULL;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_war := l_no_row_war + 1;
            ELSE
               l_adr.ADDRESSLINE01 := l_adr.ADDRESSLINE05;
               l_adr.ADDRESSLINE05 := NULL;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_war := l_no_row_war + 1;
            END IF;
         END IF;
          
         IF FN_VALIDATE_POSTCODE(t_prop(I).CUSTPOSTCODE) = 'INVALID' THEN
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'X', 'Invalid Postcode',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
                
         l_adr.POSTCODE := t_prop(I).CUSTPOSTCODE;
         l_adr.COUNTRY := t_prop(I).CUSTCOUNTRY;
         l_adr.LOCATIONFREETEXTDESCRIPTOR := t_prop(I).CUSTLOCATIONFREETEXTDESCRIPTOR;

         IF l_adr.COUNTRY IS NULL THEN 
            l_adr.FOREIGN_ADDRESS := 'N'; 
         ELSE
            l_adr.FOREIGN_ADDRESS := 'Y'; 
         END IF;
      
         l_progress := 'INSERT MO_ADDRESS';   
         BEGIN 
            INSERT INTO MO_ADDRESS
            (ADDRESS_PK, UPRN, PAFADDRESSKEY, PROPERTYNUMBERPROPERTY, CUSTOMERNUMBERPROPERTY, 
             UPRNREASONCODE, SECONDADDRESABLEOBJECT, PRIMARYADDRESSABLEOBJECT, ADDRESSLINE01, 
             addressline02, addressline03, addressline04, addressline05, postcode,
             COUNTRY, LOCATIONFREETEXTDESCRIPTOR, FOREIGN_ADDRESS)
            VALUES
            (l_adr.ADDRESS_PK, l_adr.UPRN, l_adr.PAFADDRESSKEY, l_adr.PROPERTYNUMBERPROPERTY, l_adr.CUSTOMERNUMBERPROPERTY, 
             l_adr.UPRNREASONCODE, l_adr.SECONDADDRESABLEOBJECT, l_adr.PRIMARYADDRESSABLEOBJECT, l_adr.ADDRESSLINE01, 
             l_adr.ADDRESSLINE02, l_adr.ADDRESSLINE03, l_adr.ADDRESSLINE04, l_adr.ADDRESSLINE05, l_adr.POSTCODE,
             l_adr.COUNTRY, l_adr.LOCATIONFREETEXTDESCRIPTOR, l_adr.FOREIGN_ADDRESS);
         EXCEPTION 
         WHEN OTHERS THEN 
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_err := l_no_row_err + 1;
         END;

        -- Write ADDRESS OF CUSTOMER    
        
        IF l_rec_written THEN     
           l_no_row_insert_adrc := l_no_row_insert_adrc + 1;  
           l_adr_cus.ADDRESS_PK := l_adr.ADDRESS_PK;
           l_adr_cus.STWPROPERTYNUMBER_PK := t_prop(I).STWPROPERTYNUMBER;           
           l_adr_cus.CUSTOMERNUMBER_PK := t_prop(I).STWCUSTOMERNUMBER;   
           l_adr_cus.ADDRESSUSAGEPROPERTY := 'BilledAt'; 
           l_adr_cus.EFFECTIVEFROMDATE := SYSDATE;             
           l_adr_cus.EFFECTIVETODATE := null;
           
           l_progress := 'INSERT MO_CUST_ADDRESS';     
  
           BEGIN 
             INSERT INTO MO_CUST_ADDRESS
             (ADDRESSPROPERTY_PK, ADDRESS_PK, CUSTOMERNUMBER_PK, ADDRESSUSAGEPROPERTY, 
              EFFECTIVEFROMDATE, EFFECTIVETODATE, STWPROPERTYNUMBER_PK)
             VALUES
             (ADDRESSPROPERTY_PK_SEQ.NEXTVAL, l_adr_cus.ADDRESS_PK, l_adr_cus.CUSTOMERNUMBER_PK, l_adr_cus.ADDRESSUSAGEPROPERTY,
              l_adr_cus.EFFECTIVEFROMDATE, l_adr_cus.EFFECTIVETODATE, l_adr_cus.STWPROPERTYNUMBER_PK);
           EXCEPTION 
           WHEN OTHERS THEN 
               l_rec_written := FALSE;
               l_error_number := SQLCODE;
               l_error_message := SQLERRM;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.no_instance, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_err := l_no_row_err + 1;
           END;
  
           -- keep count of records written
           IF l_rec_written THEN
              l_no_row_insert_cusadr := l_no_row_insert_cusadr + 1;
           END IF;  
        END IF;  
           
      END IF; 
        
      l_prev_prp  := t_prop(I).STWPROPERTYNUMBER;   
      l_prev_cus := t_prop(I).STWCUSTOMERNUMBER;   
     
      IF l_rec_written = FALSE THEN
         l_no_row_dropped := l_no_row_dropped + 1;
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

  -- write counts 
  l_progress := 'Writing Counts';  

  -- Premises
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP26', 910, l_no_row_read_prop, 'Distinct Eligible Properties read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP26', 920, l_no_row_dropped_prop, 'Eligible Properties dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP26', 930, l_no_row_insert_prop, 'Eligible Properties written to MO_ ELIGIBLE _PREMISE during Transform');    

  -- Supply Point
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1000, l_no_row_read, 'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1010, l_no_row_dropped, 'Dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 1020, l_no_row_insert_sp,  'Written to Table');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP29', 2630, l_no_row_cross,  'That are cross border');

  -- Customer
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP27', 940, l_no_row_read_cus,    'Distinct Eligible Customers read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP27', 950, l_no_row_dropped, 'Eligible Customers  dropped during Transform');
  p_mig_batch.fn_reconlog(no_batch, l_job.no_instance, 'CP27', 960, l_no_row_insert_cus,  'Eligible Customers written to MO_CUSTOMER during Transform');
  --P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP27', 961, l_le_row_insert,  'Legal Entities written to MO_CUSTOMER during Transform');
  
  -- Address
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 970, l_no_row_read_adrp + l_no_row_read_adrc, 'Distinct addresses read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 990, l_no_row_insert_adrp + l_no_row_insert_adrc, 'Distinct addresses inserted');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 980, l_no_row_dropped, 'Distinct addresses dropped');

  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 971,l_no_row_read_prop, 'Distinct eligible properties read');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 991,l_no_row_insert_propadr, 'Distinct eligible properties inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, 'CP28', 981,l_no_row_dropped, 'Distinct eligible properties dropped');

  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 973, l_no_row_read_cus, 'Distinct customers read');
  P_MIG_BATCH.FN_RECONLOg(no_batch, l_job.no_instance, 'CP28', 993, l_no_row_insert_cusadr, 'Distinct customers inserted');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, 'CP28', 983, l_no_row_dropped, 'Distinct customers dropped');
  
  
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
END P_FIN_TRAN_SUPPLY_POINT;
/
show error;
exit;