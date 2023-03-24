------------------------------------------------------------------------------
-- TASK					: 	APPLY PATCH007. 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	p_remove_address_pk_const.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	24/02/2016
--	
-- DESCRIPTION 		   	: 	Removes UNIQUE constraints from the following tables for column (ADDRESS_PK)
--							          MO_CUST_ADDRESS, MO_PROPERTY_ADDRESS, MO_METER_ADDRESS
--
-- NOTES  				:	None.
--
-- ASSOCIATED FILES		:	None
-- ASSOCIATED SCRIPTS  	:	None
--
-- PARAMETERES			:	None
-- USAGE				:	Not Applicable
-- EXAMPLE				:	Not Applicable
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date                Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       		08/03/2016    		N.Henderson         Initial version.
--
--
--
------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE p_remove_address_pk_const
IS
BEGIN
	FOR cur_rec IN (SELECT table_name, constraint_name, constraint_type, column_name
	from user_constraints natural join user_cons_columns
	where table_name in ('MO_CUST_ADDRESS','MO_PROPERTY_ADDRESS','MO_METER_ADDRESS') and 
	constraint_type = 'C' and column_name = 'ADDRESS_PK')
     LOOP
     -- dbms_output.put_line ('ALTER TABLE ' || cur_rec.table_name || ' DROP ' || 'CONSTRAINT ' || cur_rec.constraint_name || ';');
     EXECUTE IMMEDIATE 'ALTER TABLE ' || cur_rec.table_name || ' DROP ' || 'CONSTRAINT ' || cur_rec.constraint_name;
     END LOOP;
END;
/

exec p_remove_address_pk_const;
commit;
exit;

