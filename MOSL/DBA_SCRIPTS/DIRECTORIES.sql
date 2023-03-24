--  Subversion $Revision: 4948 $

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      20/07/2016  M.Marron   Initial Version

-----------------------------------------------------------------------------------------
--NB physical directory is different fro each enviroment so uncomment the one your need before running 

-- Give access to a folder to a user account Change create dependent on enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWD'; <--change the directory for each enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWS'; <--change the directory for each enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWP'; <--change the directory for each enviroment

grant read,write on directory FILES to MOUTRAN;
grant read,write on directory FILES TO SAPTRAN;
grant read,write on directory FILES to FINTRAN;


-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWD';
-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWS';
-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWP';

grant read,write on directory DELEXPORT TO MOUDEL;
grant read,write on directory DELEXPORT to FINDEL;


-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWD';
-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWS';
-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWP';

grant read,write on directory SAPDELEXPORT to SAPDEL;


-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWD/FINIMPORTS/SAP';
-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWS/FINIMPORTS/SAP';
-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWP/FINIMPORTS/SAP';

grant read,write on directory FINSAPIMPORT to RECEPTION;

-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWD/FINIMPORTS/OWC';
-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWS/FINIMPORTS/OWC';
-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWP/FINIMPORTS/OWC';

grant read,write on directory FINSAPIMPORT to RECEPTION;