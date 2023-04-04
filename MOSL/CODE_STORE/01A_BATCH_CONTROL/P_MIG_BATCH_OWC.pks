create or replace
PACKAGE P_MIG_BATCH_OWC AS 
 ------------------------------------------------------------------------------------
-- PACKAGE SPECIFICATION: Batch Migration
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MIG_BATCH_OWC.pks
--
-- Subversion $Revision: 5775 $
--
-- CREATED        : 22/09/2016
--
-- DESCRIPTION    : Package containing common procedures for batch migration :-
--                  batch control, job control, error logging and reconciliation counts
--
-- NOTES  :       For OWC file processing
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------        --------------------------------------
-- V 0.02      10/10/2016          S.Badhan       Send return code from P_STARTBATCH.
-- V 0.01      22/09/2016          S.Badhan       Initial Version
----------------------------------------------------------------------------------------


c_module_name                   CONSTANT VARCHAR2(30) := 'P_MIG_BATCH_OWC';
c_limit_in                      CONSTANT NUMBER(5) := 1000;
c_company_cd                    CONSTANT VARCHAR2(4) := 'STW1';
c_no_organisation               CONSTANT NUMBER(1) := 1;

g_no_batch                      MIG_BATCHSTATUS.NO_BATCH%TYPE;
g_no_job                        MIG_JOBREF.NO_JOB%TYPE;
g_proc_name                     MIG_JOBREF.NM_PROCESS%TYPE;

PROCEDURE P_STARTBATCH(no_batch          IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                       lreturn_code      IN OUT NUMBER ) ;                                      

PROCEDURE FN_UPDATEBATCH(ind_status     IN MIG_BATCHSTATUS.BATCH_STATUS%TYPE default 'RUN');

PROCEDURE FN_STARTJOB(no_batch          IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                      no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                      nm_process        IN MIG_JOBREF.NM_PROCESS%TYPE,
                      no_instance       OUT MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                      err_tolerance     OUT MIG_JOBSTATUS.ERR_TOLERANCE%TYPE,
                      exp_tolerance     OUT MIG_JOBSTATUS.EXP_TOLERANCE%TYPE,
                      war_tolerance     OUT MIG_JOBSTATUS.WAR_TOLERANCE%TYPE,
                      no_commit         OUT MIG_JOBSTATUS.NO_COMMIT%TYPE,
                      no_stream         OUT MIG_JOBSTATUS.NO_STREAM%TYPE,
                      no_range_min      OUT MIG_JOBSTATUS.NO_RANGE_MIN%TYPE,
                      no_range_max      OUT MIG_JOBSTATUS.NO_RANGE_MAX%TYPE,
                      ind_status        IN OUT MIG_JOBSTATUS.IND_STATUS%TYPE);

PROCEDURE FN_UPDATEJOB(no_batch         IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                       no_instance      IN MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                       ind_status       IN OUT MIG_JOBSTATUS.IND_STATUS%TYPE);

PROCEDURE FN_ERRORLOG(no_batch          IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                      no_instance       IN MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                      ind_log           IN MIG_ERRORLOG.IND_LOG%TYPE,
                      txt_err           IN MIG_ERRREF.TXT_ERR%TYPE,
                      txt_key           IN MIG_ERRORLOG.TXT_KEY%TYPE,
                      txt_data          IN MIG_ERRORLOG.TXT_DATA%TYPE default null);

PROCEDURE FN_RECONLOG(no_batch            IN MIG_CPLOG.NO_BATCH%TYPE,
                      no_instance         IN MIG_CPLOG.NO_INSTANCE%TYPE,
                      recon_control_point IN MIG_CPREF.RECON_CONTROL_POINT%TYPE,
                      recon_measure       IN MIG_CPREF.RECON_MEASURE%TYPE,
                      recon_measure_tot   IN MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE,
                      recon_measure_desc  IN MIG_CPREF.RECON_MEASURE_DESC%TYPE);

END P_MIG_BATCH_OWC;
/
show error;
exit;