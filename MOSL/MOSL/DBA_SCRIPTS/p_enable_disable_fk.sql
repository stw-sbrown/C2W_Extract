------------------------------------------------------------------------------
-- TASK					: 	DISABLE OR ENABLE FOREIGN KEYS ON A SPECIFIC SCHEMA 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	p_enable_disable_fk.sql
--
-- CREATED        		: 	24/02/2016
--	
-- DESCRIPTION 		   	: 	Disable or enable foreign key constraints within a specific schema
--
-- NOTES  				:	Used in conjunction with the following scripts to clear down the database.
-- 							Foreign Keys will need to be disabled before the truncate, then re-enabled
-- 							afterwards.
--
-- ASSOCIATED FILES		:	p_truncate_mo_tables.sql
-- ASSOCIATED SCRIPTS  	:	p_truncate_mo_tables.sql
--
-- PARAMETERES			:	Two parameteres required.
-- USAGE				:	execute pkg_enable_disable_fk ('ACTION','SCHEMA_NAME') where 'ACTION' is
--							'DISABLE' or 'ENABLE' and 'SCHEMA_NAME' is the name of the schema
-- EXAMPLE				:	execute p_enable_disable_fk ('DISABLE','MOUTRAN')
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




CREATE OR REPLACE PROCEDURE p_enable_disable_fk (
   status           IN   VARCHAR2,
   current_schema   IN   VARCHAR2
)
IS

BEGIN
   FOR cur_rec IN (SELECT table_name, constraint_name
                     FROM user_constraints
                    WHERE owner = current_schema
                        AND constraint_type  = 'R')
     LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE ' || cur_rec.table_name 
                                       || CASE status WHEN 'ENABLE' THEN ' ENABLE ' ELSE ' DISABLE ' END
                                       || 'CONSTRAINT ' 
                                       || cur_rec.constraint_name;
     END LOOP;
END;
/ 

