------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	LU_P0008.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	27/04/2016
--
-- Subversion $Revision: 4023 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           	 Author         	Description
-- ---------      ----------     	 -------        	------------------------------------------------
-- V0.01		    	27/04/2016		D.Cheung		        Create new lookup table for Outreader Protocols	 
--													
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ------------


-- Create LU_SPID_RANGE table
--DROP TABLE LU_OUTREADER_PROTOCOLS PURGE;
CREATE TABLE LU_OUTREADER_PROTOCOLS
(
MANUFACTURER_PK VARCHAR2(32) CONSTRAINT CH01_OUTPROTMANUF NOT NULL,
READMETHOD_PK VARCHAR2(1) CONSTRAINT CH01_OUTPROTREADMETHOD NOT NULL,
OUTREADERPROTOCOL VARCHAR2(255 BYTE) CONSTRAINT CH01_OUTPROTOCOL NOT NULL
);
--
-- set primary key
ALTER TABLE LU_OUTREADER_PROTOCOLS ADD CONSTRAINT PK_OUTPROT_MANREAD PRIMARY KEY (MANUFACTURER_PK,READMETHOD_PK);
--
-- add table and field comments
COMMENT ON Table LU_OUTREADER_PROTOCOLS IS 'This table holds the remote Protocols used by manufacturers for different read methods';
COMMENT ON COLUMN LU_OUTREADER_PROTOCOLS.MANUFACTURER_PK IS 'Meter Manufacturer';
COMMENT ON COLUMN LU_OUTREADER_PROTOCOLS.READMETHOD_PK IS 'Remote Read Method';
COMMENT ON COLUMN LU_OUTREADER_PROTOCOLS.OUTREADERPROTOCOL IS 'Remote protocol as supplier by manufacturer';

/
commit;
exit;
