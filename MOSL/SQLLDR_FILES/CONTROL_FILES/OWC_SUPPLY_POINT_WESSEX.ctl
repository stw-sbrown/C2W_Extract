--
-- Subversion $Revision: 5600 $	
--

LOAD data append into table OWC_SUPPLY_POINT
fields terminated by "|" TRAILING NULLCOLS
(
SPID_PK,
WHOLESALERID,
RETAILERID,
SERVICECATEGORY,
SUPPLYPOINTEFFECTIVEFROMDATE                     "to_date(:SUPPLYPOINTEFFECTIVEFROMDATE,'YYYY-MM-DD')",
PAIRINGREFREASONCODE,
OTHERWHOLESALERID,
MULTIPLEWHOLESALERFLAG,
DISCONRECONDEREGSTATUS,
VOABAREFERENCE,
VOABAREFRSNCODE,
UPRN,
UPRNREASONCODE,
CUSTOMERCLASSIFICATION,
PUBHEALTHRELSITEARR,
NONPUBHEALTHRELSITE,
NONPUBHEALTHRELSITEDSC,
STDINDUSTRYCLASSCODE,
STDINDUSTRYCLASSCODETYPE,
RATEABLEVALUE,
OCCUPENCYSTATUS,
BUILDINGWATERSTATUS,
LANDLORDSPID,
SECTION154,
CUSTOMERNAME,
CUSTOMERBANNERNAME,
PREMLOCATIONFREETEXTDESCRIPTOR,
PREMSECONDADDRESABLEOBJECT,
PREMPRIMARYADDRESSABLEOBJECT,
PREMADDRESSLINE01,
PREMADDRESSLINE02,
PREMADDRESSLINE03,
PREMADDRESSLINE04,
PREMADDRESSLINE05,
PREMPOSTCODE,
PREMPAFADDRESSKEY,
CUSTLOCATIONFREETEXTDESCRIPTOR,
CUSTSECONDADDRESABLEOBJECT,
CUSTPRIMARYADDRESSABLEOBJECT,
CUSTADDRESSLINE01,
CUSTADDRESSLINE02,
CUSTADDRESSLINE03,
CUSTADDRESSLINE04,
CUSTADDRESSLINE05,
CUSTPOSTCODE,
CUSTCOUNTRY,
CUSTPAFADDRESSKEY,
STWPROPERTYNUMBER,
SAPFLOCNUMBER,
STWCUSTOMERNUMBER,
OWC                  CONSTANT 'WESSEX-W'
)
