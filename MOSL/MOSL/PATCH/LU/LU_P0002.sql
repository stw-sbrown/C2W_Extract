------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P00002.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	02/03/2016
--
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Patch to LU_PUBHEALTHRESITE:
--													Problem description: Missing field called "SENSITIVE"
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ==============
Drop table LU_PUBHEALTHRESITE purge;

-- Create LU_PUBHEALTHRESITE table
CREATE TABLE LU_PUBHEALTHRESITE
(
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
NONPUBHEALTHRELSITE  NUMBER(1) NOT NULL,
NONPUBHEALTHRELSITEDSC VARCHAR(255) ,
PUBHEALTHRELSITEARR NUMBER(1) NOT NULL,
SENSITIVE VARCHAR(5) NOT NULL
);
--
-- set primary key & Constraints
ALTER TABLE LU_PUBHEALTHRESITE ADD PRIMARY KEY (STWPROPERTYNUMBER_PK);
ALTER TABLE LU_PUBHEALTHRESITE ADD CONSTRAINT LU_CHK01_NONPUBHEALTHRELSITE  CHECK (NONPUBHEALTHRELSITE  IN (0,1));
ALTER TABLE LU_PUBHEALTHRESITE ADD CONSTRAINT LU_CHK01_PUBHEALTHRELSITEARR CHECK (PUBHEALTHRELSITEARR IN (0,1));
ALTER TABLE LU_PUBHEALTHRESITE ADD CONSTRAINT LU_CHK01_SENSITIVE CHECK (SENSITIVE IN ('Y','N'));
--
-- add table and field comments
COMMENT ON Table "LU_PUBHEALTHRESITE" IS 'This table holds data about SENSITIVE properties, source externally from Target but required from MOSL upload';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.NONPUBHEALTHRELSITE  IS 'Non-Public Health Related Site Specific Arrangements Flag~~~D2093 - Indication of whether or not a site specific management plan is in place, and not for public health related reasons';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.NONPUBHEALTHRELSITEDSC IS 'Non-Public Health Related Site Specific Arrangements Free Descriptor~~~D2094 - Free descriptor for indication of the nature of site specific management plan in place, when not for public health related reasons';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.PUBHEALTHRELSITEARR IS 'Public Health Related Site Specific Arrangements Flag~~~D2087 - Boolean flag to Indicate whether or not a site specific management plan is in place for public health related reasons';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.SENSITIVE IS 'Sensitive Customer Classification~~~D2005 - Customer classification for a Supply Point, for identification of where a customer is defined as vulnerable for the purposes of the Security and Emergency Measures (Water and Sewerage Undertakers) Directions ';

commit;
exit;



