CREATE OR REPLACE PROCEDURE P_MOU_TRAN_SUPPLY_POINT(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                    no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                    return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Supply Point MO transformation extract
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SUPPLY_POINT.sql
--
-- Subversion $Revision: 4023 $
--
-- CREATED        : 10/03/2016
--
-- DESCRIPTION    : Procedure to create the upply Point MO transformation extract
--                 Will read from key gen and target tables, apply any transformationn
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 1.03      16/05/2016  S.Badhan   Output counts for cross border supplies 
-- V 1.02      26/04/2016  S.Badhan   Issue I-195. Report where more than one customer 
--                                    per supply point.
-- V 1.01      21/04/2016  S.Badhan   Fixed Issue I-110 Conditional setting of pairing 
--                                    reference reason and other wholesaler id.
-- V 1.00      22/03/2016  S.Badhan   Use CORESPID_PK as key to LU_SPID_RANGE
-- V 0.02      15/03/2016  S.Badhan   keygen table renamed to BT_TVP054
-- V 0.01      10/03/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SUPPLY_POINT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_prev_spt                    LU_SERVICE_CATEGORY.SUPPLY_POINT_CODE%TYPE; 
  l_prev_cus                    BT_TVP054.NO_LEGAL_ENTITY%TYPE;   
  l_spt                         LU_SERVICE_CATEGORY.SUPPLY_POINT_CODE%TYPE;    
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_sp                          MO_SUPPLY_POINT%ROWTYPE; 
  l_spid                        LU_SPID_RANGE%ROWTYPE; 
  l_lnd                         LU_LANDLORD%ROWTYPE;
  l_brd                         LU_CROSSBORDER%ROWTYPE;    
  l_cat                         LU_SERVICE_CATEGORY%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_sp              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_cross                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;    
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_water_sp                    NUMBER(9);
  l_sewage_sp                   NUMBER(9);
  l_cross_border                BOOLEAN;  
  
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
   IS 
   SELECT  DISTINCT
           t054.CD_COMPANY_SYSTEM,
           t054.NO_PROPERTY,
           t054.NO_SERV_PROV, 
           t054.NO_LEGAL_ENTITY, 
           t054.DT_START, 
           t054.DT_END, 
           t054.CORESPID,
           t054.SAP_FLOC,
           tcat.SUPPLY_POINT_CODE,
           t056.DT_STATUS,
           t056.ST_SERV_PROV,    
           t056.DT_START AS T056_DT_START
    FROM   BT_TVP054           t054,
           LU_SERVICE_CATEGORY tcat,
           CIS.TVP056SERVPROV  t056
    WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end                 
    AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV 
    AND    t056.CD_COMPANY_SYSTEM = t054.CD_COMPANY_SYSTEM
    AND    t056.NO_PROPERTY       = t054.NO_PROPERTY
    AND    t056.NO_SERV_PROV      = t054.NO_SERV_PROV
    ORDER BY NO_PROPERTY,
             SUPPLY_POINT_CODE,
             T056_DT_START,
             t054.DT_END DESC NULLS FIRST;     
     
TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
BEGIN
 
   -- initial variables 
   
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_read_sp := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_no_row_cross := 0;
   l_prev_prp := 0;
   l_prev_spt := 'A';
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
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY || ',' || t_prop(i).NO_SERV_PROV || ',' || t_prop(i).SUPPLY_POINT_CODE || ',' || t_prop(i).NO_LEGAL_ENTITY;
      l_sp := NULL;
      l_rec_written := TRUE;
      
      -- keep count of all records read
      l_no_row_read := l_no_row_read + 1;
         
      IF l_prev_prp || l_prev_spt || l_prev_cus <> t_prop(i).NO_PROPERTY || t_prop(i).SUPPLY_POINT_CODE || t_prop(i).NO_LEGAL_ENTITY THEN
 
            -- Count of property/service category
            l_no_row_read_sp := l_no_row_read_sp + 1;

            --  Get SPID for service category 
            l_progress := 'SELECT LU_SPID_RANGE ';           
            BEGIN 
              SELECT SPID_PK
              INTO   l_spid.SPID_PK
              FROM   LU_SPID_RANGE
              WHERE  CORESPID_PK     = t_prop(i).CORESPID
              AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;	
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_spid.SPID_PK := 0;
            END;

            --  Check if cross border supply to get retailer and Wholesaler
            --  else use Severn Trent

            l_progress := 'SELECT LU_CROSSBORDER ';                
      
            l_cross_border := true;
            BEGIN 
               SELECT RETAILERID_PK, 
                      WHOLESALERID_PK
               INTO   l_brd.RETAILERID_PK,
                      l_brd.WHOLESALERID_PK
               FROM   LU_CROSSBORDER
               WHERE  (   SPID_PK     = l_spid.SPID_PK
                       OR CORESPID_PK = t_prop(i).CORESPID)
               AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;	                
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_brd.RETAILERID_PK := 'SEVERN-R';
                 l_brd.WHOLESALERID_PK := 'SEVERN-W';
                 l_cross_border := false;
            END;
            l_sp.RETAILERID_PK := l_brd.RETAILERID_PK;
            l_sp.WHOLESALERID_PK := l_brd.WHOLESALERID_PK;
                     
           -- set disconnection flag

            l_progress := 'DETERMINE DISCONNECTION STATUS ';    
            l_sp.SUPPLYPOINTEFFECTIVEFROMDATE := t_prop(i).T056_DT_START;
             
             CASE   t_prop(i).ST_SERV_PROV
               WHEN 'D' THEN
                    l_sp.DISCONRECONDEREGSTATUS  := 'TDISC';
               WHEN 'I' THEN
                     l_sp.DISCONRECONDEREGSTATUS := 'SPERR';
               WHEN 'R' THEN
                     l_sp.DISCONRECONDEREGSTATUS := 'PDISC';
               WHEN 'X' THEN 
                     l_sp.DISCONRECONDEREGSTATUS := 'NOSP';
               ELSE 
                     l_sp.DISCONRECONDEREGSTATUS := 'REC';
             END CASE;
             
             IF l_sp.DISCONRECONDEREGSTATUS NOT IN ('TDISC', 'REC') THEN
                l_rec_written := FALSE;
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'INVALID DISCONNECTION STATUS',  l_err.TXT_KEY || ',' || l_sp.DISCONRECONDEREGSTATUS, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_exp := l_no_row_exp + 1;
             END IF;
   
            -- set fields with default values
           
             l_sp.REGISTRATIONSTARTDATE := NULL;
             l_sp.OTHERSERVICECATPROVIDED := 0;
             l_sp.OTHERSERVICECATPROVIDEDREASON := null;
             l_sp.MULTIPLEWHOLESALERFLAG := 0;
             l_sp.SPIDSTATUS := 'TRADEABLE';
             l_sp.NEWCONNECTIONTYPE := NULL;
             l_sp.ACCREDITEDENTITYFLAG := 0;
             l_sp.GAPSITEALLOCATIONMETHOD := NULL;
             l_sp.OTHERSPID := NULL;             
             l_sp.LATEREGAPPLICATION := 0;
             l_SP.VOLTRANSFERFLAG := 0;
 
             -- code for landlord SPID  - use lookup table
             l_progress := 'SELECT LU_LANDLORD '; 
             BEGIN 
                SELECT LANDLORDSPID
                INTO   l_lnd.LANDLORDSPID
                FROM   LU_LANDLORD
                WHERE  STWPROPERTYNUMBER_PK = t_prop(i).NO_PROPERTY
                AND    SERVICECATEGORY      = t_prop(i).SUPPLY_POINT_CODE;	 
             EXCEPTION
             WHEN NO_DATA_FOUND THEN
                  l_lnd.LANDLORDSPID := NULL;
              END;

             IF t_prop(i).SUPPLY_POINT_CODE = 'S' THEN
                l_cat.SUPPLY_POINT_CODE := 'W';
             ELSE
                l_cat.SUPPLY_POINT_CODE := 'S';             
             END IF;
             
             -- check for another supply point
             l_progress := 'SELECT OTHER SUPPLY POINT '; 
             l_sp.PAIRINGREFREASONCODE := NULL;
             BEGIN 
                SELECT SUPPLY_POINT_CODE
                INTO   l_spt
                FROM   BT_TVP054         t054,
                       LU_SERVICE_CATEGORY tcat
                WHERE  tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                AND    t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                AND    tcat.SUPPLY_POINT_CODE     = l_cat.SUPPLY_POINT_CODE
                GROUP BY SUPPLY_POINT_CODE;
             EXCEPTION
             WHEN NO_DATA_FOUND THEN
                  l_sp.PAIRINGREFREASONCODE := 'NOSPID';
             END;

            --  Get other wholesaler id

             IF l_sp.PAIRINGREFREASONCODE IS NULL THEN
                l_progress := 'SELECT LU_CROSSBORDER OTHER';                
          
                BEGIN 
                   SELECT WHOLESALERID_PK
                   INTO   l_brd.WHOLESALERID_PK
                   FROM   LU_CROSSBORDER
                   WHERE  CORESPID_PK     = t_prop(i).CORESPID
                   AND    SERVICECATEGORY = l_cat.SUPPLY_POINT_CODE;	                
                EXCEPTION
                WHEN NO_DATA_FOUND THEN
                     l_brd.WHOLESALERID_PK := 'SEVERN-W';
                END;
                    
                l_sp.OTHERWHOLESALERID := l_brd.WHOLESALERID_PK;
             ELSE
                l_sp.OTHERWHOLESALERID := null;          
             END IF;
             
            -- Write suppy point record
             l_progress := 'WRITING MO_SUPPLY_POINT ';             

            
            IF l_rec_written THEN
                BEGIN 
                  INSERT INTO MO_SUPPLY_POINT
                  (SPID_PK, STWPROPERTYNUMBER_PK, CORESPID_PK, RETAILERID_PK, WHOLESALERID_PK, CUSTOMERNUMBER_PK,
                   SAPFLOCNUMBER, SERVICECATEGORY, SUPPLYPOINTEFFECTIVEFROMDATE, REGISTRATIONSTARTDATE,
                   DISCONRECONDEREGSTATUS, OTHERSERVICECATPROVIDED, OTHERSERVICECATPROVIDEDREASON, MULTIPLEWHOLESALERFLAG,
                   LANDLORDSPID, SPIDSTATUS, NEWCONNECTIONTYPE, ACCREDITEDENTITYFLAG, GAPSITEALLOCATIONMETHOD, OTHERSPID,
                   OTHERWHOLESALERID, PAIRINGREFREASONCODE, LATEREGAPPLICATION, VOLTRANSFERFLAG, SUPPLYPOINTREFERENCE)
                  VALUES
                  (l_spid.SPID_PK, t_prop(i).NO_PROPERTY, t_prop(i).CORESPID, l_sp.RETAILERID_PK, l_sp.WHOLESALERID_PK, t_prop(i).NO_LEGAL_ENTITY,
                   t_prop(i).SAP_FLOC, t_prop(i).SUPPLY_POINT_CODE, l_sp.SUPPLYPOINTEFFECTIVEFROMDATE, l_sp.REGISTRATIONSTARTDATE,
                   l_sp.DISCONRECONDEREGSTATUS, l_sp.OTHERSERVICECATPROVIDED, l_sp.OTHERSERVICECATPROVIDEDREASON, l_sp.MULTIPLEWHOLESALERFLAG,
                   l_lnd.LANDLORDSPID, l_sp.SPIDSTATUS, l_sp.NEWCONNECTIONTYPE, l_sp.ACCREDITEDENTITYFLAG, l_sp.GAPSITEALLOCATIONMETHOD, l_sp.OTHERSPID,
                   l_sp.OTHERWHOLESALERID, l_sp.PAIRINGREFREASONCODE, l_sp.LATEREGAPPLICATION, l_sp.VOLTRANSFERFLAG, t_prop(i).CORESPID);
                EXCEPTION 
                WHEN DUP_VAL_ON_INDEX THEN
                     l_no_row_dropped := l_no_row_dropped + 1;
                     l_rec_written := FALSE;
                     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 LE for Supply Point',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));                     
                     l_no_row_exp := l_no_row_exp + 1;
                WHEN OTHERS THEN 
                     l_no_row_dropped := l_no_row_dropped + 1;
                     l_rec_written := FALSE;
                     l_error_number := SQLCODE;
                     l_error_message := SQLERRM;
                     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                     l_no_row_err := l_no_row_err + 1;
                END;
            END IF;
            
            -- keep count of records written
            IF l_rec_written THEN
               l_no_row_insert := l_no_row_insert + 1;
               IF l_cross_border THEN
                  l_no_row_cross := l_no_row_cross + 1;
               END IF;
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
       
            l_prev_spt := t_prop(i).SUPPLY_POINT_CODE;
            l_prev_prp := t_prop(i).NO_PROPERTY;
            l_prev_cus := t_prop(i).NO_LEGAL_ENTITY;
                         
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
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP29', 1000, l_no_row_read_sp, 'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP29', 1010, l_no_row_dropped, 'Dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP29', 1020, l_no_row_insert,  'Written to Table'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP29', 2630, l_no_row_cross,  'That are cross border'); 
  

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
END P_MOU_TRAN_SUPPLY_POINT;
/
show error;

exit;