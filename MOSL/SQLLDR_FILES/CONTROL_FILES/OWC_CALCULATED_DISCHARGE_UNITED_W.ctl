--
-- Subversion $Revision: 5800 $	
--
LOAD data append into table OWC_CALCULATED_DISCHARGE_W
fields terminated by "|" TRAILING NULLCOLS
(
DPID_PK,
CALCDISCHARGEID_PK,
DISCHARGETYPE,
SUBMISSIONFREQ,
TEYEARLYVOLESTIMATE,
OWC                  CONSTANT 'UNITED-W'
)
