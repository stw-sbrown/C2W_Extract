------------------------------------------------------------------------------
-- TASK					: 	TRUNCATE ALL TABLES IN A SCHEMA THAT START "MO" 
--							This executes the SP as well
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	p_truncate_mo_tables
--
-- CREATED        		: 	24/02/2016
--	
-- DESCRIPTION 		   	: 	Truncate data from tables where table name starts "MO"
--
-- NOTES  				:	Used in conjunction with the following scripts to clear down the database.
-- 							Foreign Keys will need to be disabled before the truncate, then re-enabled
-- 							afterwards.
--
-- ASSOCIATED FILES		:	p_enable_disable_fk.sql
-- ASSOCIATED SCRIPTS  	:	p_enable_disable_fk.sql
--
-- PARAMETERES			:	None needed
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date                Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       		24/02/2016    		N.Henderson         Initial version.
--
--
--
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE p_truncate_mo_tables
IS

BEGIN
   FOR cur_rec IN (select table_name from user_tables where table_name like 'MO%')                
     LOOP
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || cur_rec.table_name;
     END LOOP;
END;
/ 

exec p_truncate_mo_tables