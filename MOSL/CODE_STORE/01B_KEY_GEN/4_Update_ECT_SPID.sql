SET SERVEROUTPUT ON;
--
-- Update property spid to unique value
-- 18/03/2015
-- BY Surinder Badhan
--
  ---  !!!! note ECT table contains    - 404,698
  ---            ECT required rows     - 249,184
  --             LU_SPID_RANGE distinct CORESPID_PK contains  300,00 rows
  --        if ECT required rows >  LU_SPID_RANGE unique CORESPID_PK 
  --           then there will duplicate COREPID's for the property
  

DECLARE
CURSOR cur_prop           
    IS 
    SELECT  distinct
            t054.NO_PROPERTY
     FROM   ELIGIBILITY_CONTROL_TABLE t054
     WHERE  CD_PROPERTY_USE_FUT  NOT IN ('X', 'E', 'D', 'H')
     AND    VALIDATED_FLAG <> 'D' 
     --where  no_property = 1002046;
     order by t054.NO_PROPERTY;

CURSOR cur_spid           
    IS 
    SELECT  DISTINCT
            CORESPID_PK
     FROM   LU_SPID_RANGE
     ORDER BY CORESPID_PK;
     
TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;

 l_error_number                VARCHAR2(255);
 l_error_message               VARCHAR2(512);  
 l_progress                    VARCHAR2(100);
 l_prev_prp                    ELIGIBILITY_CONTROL_TABLE.NO_PROPERTY%TYPE; 
 l_prev_corespid               LU_SPID_RANGE.CORESPID_PK%TYPE := '0';  
 l_job                         MIG_JOBSTATUS%ROWTYPE;
 l_err                         MIG_ERRORLOG%ROWTYPE; 
 l_spd                         LU_SPID_RANGE%ROWTYPE;  
 l_mo                          MO_ELIGIBLE_PREMISES%ROWTYPE; 
 nLCount                         PLS_INTEGER := 0;
 nLCommitFrequency               PLS_INTEGER := 100000;
  
    
BEGIN

 --DBMS_OUTPUT.PUT_LINE( t_prop(i).NO_PROPERTY || ' ' || l_spd.CORESPID_PK);
 
   OPEN cur_SPID; 
  
   FOR  cur_rec IN cur_prop
   LOOP

       l_progress := 'loop processing ';

       FETCH cur_spid
       INTO  l_spd.CORESPID_PK;
    
       IF  l_spd.CORESPID_PK = l_prev_corespid THEN
           DBMS_OUTPUT.PUT_LINE( cur_rec.NO_PROPERTY || ' ' || l_spd.CORESPID_PK);
       END IF;
    
       UPDATE ELIGIBILITY_CONTROL_TABLE
       SET    CORESPID    = l_spd.CORESPID_PK
       WHERE  NO_PROPERTY = cur_rec.NO_PROPERTY;
       
       l_prev_corespid := l_spd.CORESPID_PK;
       

    nLCount := nLCount + 1;
    --
    IF MOD(nLCount, nLCommitFrequency) = 0 THEN
       COMMIT;
    END IF;
    --
  END LOOP;

  CLOSE cur_SPID; 
   
  -- write counts 
  l_progress := 'Writing Counts';  
  
  COMMIT;  
   
EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     
    DBMS_OUTPUT.PUT_LINE('ERROR ' || l_error_number || ' ' || l_error_message);

END ;

commit;

exit;

