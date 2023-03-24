--
-- Subversion $Revision: 4023 $	
--
load data
into table LU_TARIFF_SPECIAL_AGREEMENTS
fields terminated by "|"
TRAILING NULLCOLS
(
PROPERTY_NO,                                                                                                                                                                                               
ACCOUNT_NO,                                                                                                                                                                                                
CUSTOMER_NAME,                                                                                                                                                                                             
TARIFFCODE,                                                                                                                                                                                                
SERVICECOMPONENTTYPE,                                                                                                                                                                                      
SPECIAL_AGREEMENT_FLAG,                                                                                                                                                                                    
SPECIAL_AGREEMENT_DESC,                                                                                                                                                                                    
OFWAT_REFERENCE_NUMBER,                                                                                                                                                                                    
STW_REFERENCE_NUMBER,                                                                                                                                                                                  
SPECIAL_AGREEMENT_FACTOR
)