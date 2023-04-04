--
-- Subversion $Revision: 5675 $	
--
load data
into table LU_OWC_RECON_MEASURES
fields terminated by "|"
(
    OWC,
    CONTROL_POINT,
    MO_TABLE,
    OBJ_READ_MEASURE,
    OBJ_DROPPED_MEASURE,
    OBJ_INSERTED_MEASURE 
)
