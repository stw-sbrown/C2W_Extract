SET SERVEROUTPUT ON; 
 /*------------------------------------------------------------------------------
|| FOR TESTING PROCS
||----------------------------------------------------------------------------*/

DECLARE

c_module_name                   CONSTANT VARCHAR2(30) := 'CALL_PROC';
c_limit_in                      CONSTANT NUMBER(5) := 10000;
l_error_number                  VARCHAR2(255);
l_error_message                 VARCHAR2(512);
l_key                           VARCHAR2(200);
l_prp                           CIS.TVP046PROPERTY%ROWTYPE;
l_progress                      VARCHAR2(100);
return_code                     NUMBER;  

/*-------------------------------------------------------------------------------------------------------------
| Main processing. 
|-------------------------------------------------------------------------------------------------------------*/
BEGIN

  DBMS_OUTPUT.enable(1000000);
  DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  PROC Started');

--  P_MIG_BATCH.P_STARTBATCH;

   return_code := 0;
   
  -- P_MOU_TRAN_KEY_GEN(11, 100, return_code);         --key gen

--831174712
--607002254
--908074174

/*
     UPDATE MIG_JOBREF
     SET    NO_RANGE_MIN = 831174712,
            NO_RANGE_MAX = 831174712
     WHERE  NM_PROCESS = 'P_MOU_TRAN_PROPERTY';
*/

   P_MOU_TRAN_PROPERTY(15, 200, return_code);         --property extract
      
   IF return_code = -1 THEN
      DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'HH24:MI:SS') || ' -  PROC Ended With errors  !!!!!');
   ELSE 
      DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'HH24:MI:SS') || ' -  PROC Ended ');
   END IF;

  COMMIT;     

  --
EXCEPTION
WHEN OTHERS THEN    
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     DBMS_OUTPUT.PUT_LINE('Error in ' || c_module_name || ' at step ' || l_progress || ' -  ' || l_key || ' - ' || l_error_message);
--     DBMS_OUTPUT.PUT_LINE( DBMS_UTILITY.FORMAT_ERROR_STACK );
--     DBMS_OUTPUT.PUT_LINE( DBMS_UTILITY.FORMAT_ERROR_BACKTRACE );
 --    RAISE PROGRAM_ERROR;
     -- 
END;
/
