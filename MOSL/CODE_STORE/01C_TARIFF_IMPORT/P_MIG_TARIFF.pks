create or replace
PACKAGE P_MIG_TARIFF AS
 ------------------------------------------------------------------------------------
-- PACKAGE SPECIFICATION: Batch Migration
--
-- AUTHOR         : sreedhar p
--
-- FILENAME       : P_MIG_TARIFF.pks
--
-- Subversion $Revision: 4920 $
--
-- CREATED        : 3/mar/2016
--
-- DESCRIPTION    :
--
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------        --------------------------------------
-- V 0.01      3/mar/2016          S.Pallati       Initial Version
-- V 0.02      15/mar/2016         S.Pallati       implemented mike review comments like using
--                                                 upper case for status, error tolerance
----------------------------------------------------------------------------------------


procedure P_MOU_TRAN_TARIFF_AS   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );

procedure P_MOU_TRAN_TARIFF_AW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );

procedure P_MOU_TRAN_TARIFF_MPW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );

procedure P_MOU_TRAN_TARIFF_MS   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );
procedure P_MOU_TRAN_TARIFF_SW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );
procedure P_MOU_TRAN_TARIFF_TE   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );
procedure P_MOU_TRAN_TARIFF_US   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );
procedure P_MOU_TRAN_TARIFF_UW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );

procedure P_MOU_TRAN_TARIFF_HD   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER );


procedure P_MOU_TRAN_TARIFF_RUN;

g_err_tol number := 1;
G_ERR_ROWS NUMBER := 0;
g_state varchar2(10) := 'Verified';
END P_MIG_TARIFF;
/
exit;
