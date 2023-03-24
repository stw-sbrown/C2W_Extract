-- TASK			: 	MIG RDBMS Create 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	01_DDL_CREATE_MIG_VIEWS.sql
--
-- CREATED        		: 	10/03/2016
--
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		: 	CREATE all MIG VIEWS
--
-- NOTES  			:	
--
-- ASSOCIATED SCRIPTS  	:	
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------            	 ------------------------------------------------
-- V0.01       	10/03/2016    	N.Henderson     	Initial version
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FORCE VIEW MIG_V_BATCHSTATUS (NO_BATCH, TS_START, DT_PROCESS, TS_UPDATE, IND_STATUS, NM_PROCESS)
AS
  SELECT bs.NO_BATCH,
    st.TS_START,
    st.DT_PROCESS,
    st.TS_UPDATE,
    st.IND_STATUS,
    jb.NM_PROCESS
  FROM MIG_BATCHSTATUS bs,
    MIG_JOBREF jb,
    MIG_JOBSTATUS st
  WHERE st.NO_BATCH   = bs.NO_BATCH
  AND bs.BATCH_STATUS = 'RUN'
  AND st.NO_INSTANCE BETWEEN NO_JOB AND (NO_JOB + 50);

commit;

CREATE OR REPLACE FORCE VIEW MIG_V_ERRLOG (NO_BATCH, NO_INSTANCE, TS_CREATED, NO_SEQ, IND_LOG, NO_ERR, TXT_KEY, TXT_DATA, TXT_ERR)
AS
  SELECT lg.NO_BATCH,
    lg.NO_INSTANCE,
    lg.TS_CREATED,
    lg.NO_SEQ,
    lg.IND_LOG,
    lg.NO_ERR,
    lg.TXT_KEY,
    lg.TXT_DATA,
    er.TXT_ERR
  FROM MIG_ERRORLOG lg,
    MIG_ERRREF er
  WHERE lg.NO_ERR = er.NO_ERR;
  COMMENT ON TABLE MIG_V_ERRLOG
IS
  'MIG_V_ERRLOG';

commit;

exit;

