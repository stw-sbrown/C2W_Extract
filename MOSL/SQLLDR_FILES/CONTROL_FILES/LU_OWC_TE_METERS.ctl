--
-- Subversion $Revision: 5911 $	
--
load data
into table LU_OWC_TE_METERS
fields terminated by "|"
(
    ACCOUNT_NUMBER,
    STW_PROPERTYNUMBER,
    OWC_SPID,
    OWC_METERSERIAL,
    OWC_METERMANUFACTURER,
    OWC_PROPERTYNUMBER,
    QUIS,
    OWC,
    MO_RDY
)
