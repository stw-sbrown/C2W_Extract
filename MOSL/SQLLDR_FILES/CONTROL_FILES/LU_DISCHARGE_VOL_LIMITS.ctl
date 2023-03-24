--
-- Subversion $Revision: 4948 $	
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.02       		21/07/2016     S.Badhan  SAP CR_020 - Add DPID_TYPE Field
------------------------------------------------------------------------------------------------------------

load data
into table LU_DISCHARGE_VOL_LIMITS
fields terminated by "|"
TRAILING NULLCOLS
(
NO_IWCS,
DPID_TYPE,
VOLUME_LIMIT
)
