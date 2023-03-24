-- Drop FINTRAN procs
--
-- S.BADHN
--	
-- Subversion $Revision: 5350 $
--
--16/08/2016
--
----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      16/08/2016  S.Badhan   Initial Version
----------------------------------------------------------------------------------------

PROMPT '01_DDL_DROP_FINTRAN_PROCS';
PROMPT 'Dropping Procs';

DROP PROCEDURE P_FIN_TRAN_SUPPLY_POINT;
DROP PROCEDURE P_FIN_TRAN_SERVICE_COMPONENT;
DROP PROCEDURE P_FIN_TRAN_CALC_DISCHARGE;
DROP PROCEDURE P_FIN_TRAN_DISCHARGE_POINT;
DROP PROCEDURE P_FIN_TRAN_METER; 
DROP PROCEDURE P_FIN_TRAN_METER_READING;   
DROP PROCEDURE P_FIN_TRAN_METER_SPID_ASSOC;      
DROP PROCEDURE P_FIN_TRAN_METER_NETWORK;  
DROP PROCEDURE P_FIN_TRAN_METER_DPID_XREF;  

commit;
exit;




