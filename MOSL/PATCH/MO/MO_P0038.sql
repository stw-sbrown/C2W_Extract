------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	L.Smith
--
-- FILENAME       		: 	P00038.sql
--
--
-- CREATED        		: 	13/06/2016
--	
-- DESCRIPTION 		   	: 	This file recreates index and constraint SYS_C00106150 as composite on CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author    Description
-- ---------      ----------     -------   ------------------------------------------------
-- V0.02       		13/10/2016     S.Badhan  Remove drop of SYS_C00106150, does not exist yet.
-- V0.01       		13/06/2016     L.Smith   recreates index and constraint SYS_C00106150 as composite on CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK
------------------------------------------------------------------------------------------------------------

--ALTER TABLE MO_CUST_ADDRESS DROP CONSTRAINT SYS_C00106150;

--DROP INDEX SYS_C00106150;

CREATE UNIQUE INDEX SYS_C00106150 ON MO_CUST_ADDRESS
  (
    CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK
  )
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS STORAGE
  (
    INITIAL 26214400 NEXT 26214400 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  );
  
ALTER TABLE MO_CUST_ADDRESS ADD CONSTRAINT SYS_C00106150 UNIQUE (CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK);

EXIT;
