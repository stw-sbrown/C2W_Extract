------------------------------------------------------------------------------
-- TASK					      : 	MOSL RDBMS DROP 
--
-- AUTHOR         		: 	Lee Smith
--
-- FILENAME       		: 	06_DDL_MOSL_DROP_MATERIALIZED_VIEWS.sql
--
-- CREATED        		: 	13/10/2016
--
-- Subversion $Revision: 6285 $
--	
--
-- DESCRIPTION 		   	: 	Drops all materialized views
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version  Date        Author       Description
-- -------  ----------  -------      ------------------------------------------------
-- V0.01    13/10/2016  Lee Smith    Initial version after generation from Excel
-- V0.02    18/11/2016  Lee Smith    New views 5 and 6
-- V0.03    18/11/2016  Lee Smith    Added revision

-- TE_MATCHED_WATER_METERS1_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS1_MV;
-- TE_MATCHED_WATER_METERS2_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS2_MV;
-- TE_MATCHED_WATER_METERS3_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS3_MV;
-- TE_MATCHED_WATER_METERS4_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS4_MV;
-- TE_MATCHED_WATER_METERS5_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS5_MV;
-- TE_MATCHED_WATER_METERS6_MV
DROP MATERIALIZED VIEW TE_MATCHED_WATER_METERS6_MV;


commit;
exit;
