--
-- Subversion $Revision: 5649 $	
--
LOAD data append into table OWC_CALCULATED_DISCHARGE
fields terminated by "|" TRAILING NULLCOLS
(
DPID_PK,
CALCDISCHARGEID_PK,
DISCHARGETYPE,
SUBMISSIONFREQ,
TEYEARLYVOLESTIMATE,
OWC                  CONSTANT 'ANGLIAN-W'
)
