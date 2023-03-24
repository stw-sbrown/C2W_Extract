------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS CREATION 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	05_COMMENTS_TABLES_FIELDS_ALL.sql
--
-- CREATED        		: 	22/02/2016
--
--	
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		   	: 	Adds comments to tables and fields. 
--
-- NOTES  				:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order.
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  	:	01_DDL_MOSL_TABLES_ALL.sql
--							02_DDL_MOSL_PK_ALL.sql
--							03_DDL_MOSL_FK_ALL.sql
--							04_DDL_MOSL_STATIC_DATA_ALL.sql
--
--
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       	15/02/2016    	N.Henderson         Initial version after generation from Excel
--
-- V0.02		25/02/2016		N.Henderson		Had to replace all comments symbols with --
--									Doing something wierd when running query from
--									command line.
--
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--ALTER_TABLE_DDL
--CORE
COMMENT ON TABLE MO_CUSTOMER IS 'CUSTOMER TABLE. THIS WAS NOT PART OF THE ORIGIAL SUITE OF ENTITY OBJECTS BUT WAS ADDED DURING LOGICAL AND PHYSICAL DATA MODEL INVESTIGATION';
COMMENT ON COLUMN MO_CUSTOMER.CUSTOMERNUMBER_PK IS 'Customer Number~~~STW017 - Legal Entity Number Unique identifier for the CUSTOMER';
COMMENT ON COLUMN MO_CUSTOMER.COMPANIESHOUSEREFNUM IS 'Companies House Number~~~STW018 - Companies House Registration Numbers Internal check value';
COMMENT ON COLUMN MO_CUSTOMER.CUSTOMERCLASSIFICATION IS 'Customer Classification~~~D2005 - Customer classification for a Supply Point, for identification of where a customer is defined as vulnerable for the purposes of the Security and Emergency Measures (Water and Sewerage Undertakers) Directions ';
COMMENT ON COLUMN MO_CUSTOMER.CUSTOMERNAME IS 'Customer Name~~~D2027 - The customer name associated with a given Supply Point';
COMMENT ON COLUMN MO_CUSTOMER.CUSTOMERBANNERNAME IS 'Customer Banner Name~~~D2050 - The Trading Name of the Customer at a given Eligible Premises, if known';
COMMENT ON COLUMN MO_CUSTOMER.STDINDUSTRYCLASSCODE IS 'Standard Industrial Classification Code~~~D2008 - Standard Industrial Classification Code applicable to a Supply Point';
COMMENT ON COLUMN MO_CUSTOMER.STDINDUSTRYCLASSCODETYPE IS 'Standard Industrial Classification Code Type~~~D2092 - Identifies the version of the Standard Industrial Classification Code provided';
COMMENT ON TABLE MO_ELIGIBLE_PREMISES IS 'Eligible Premise---A location that has a unique core Service Point ID (core SPID) . May equate to a physical property, but may be a sub-division of a building (e.g. a building split into a number of units)  or a series of buildings treated holistically (e.g. a retail park). What defines a premise to be eligible is dictated by OFWAT guidelines.';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.CUSTOMERID_PK IS 'Customer Number~~~STW017 - Legal Entity Number Unique identifier for the CUSTOMER';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable joins';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.RATEABLEVALUE IS 'Rateable Value~~~D2011 - Rateable Value of Eligible Premises in £';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.PROPERTYUSECODE IS 'Property Use Code~~~STW013 - Identifies whether the property is Domestic, Commercial, Industrial etc. Used to identify eligible premises';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.OCCUPENCYSTATUS IS 'Occupancy Status~~~D2015 - Declares premises for the SPID as Vacant or Occupied';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.VOABAREFERENCE IS 'VOA BA Reference~~~D2037 - Valuation Office Agency Billing Authority Reference Number';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.VOABAREFRSNCODE IS 'VOA BA Reference Reason Code~~~D2038 - Code to explain the absence or duplication of a Valuation Office Agency Billing Authority Reference. (in valid set)';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.BUILDINGWATERSTATUS IS 'Building Water Status~~~D2029 - Boolean flag to indicate if the site is a building construction site. ';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.NONPUBHEALTHRELSITE  IS 'Non-Public Health Related Site Specific Arrangements Flag~~~D2093 - Indication of whether or not a site specific management plan is in place, and not for public health related reasons';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.NONPUBHEALTHRELSITEDSC IS 'Non-Public Health Related Site Specific Arrangements Free Descriptor~~~D2094 - Free descriptor for indication of the nature of site specific management plan in place, when not for public health related reasons';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.PUBHEALTHRELSITEARR IS 'Public Health Related Site Specific Arrangements Flag~~~D2087 - Boolean flag to Indicate whether or not a site specific management plan is in place for public health related reasons';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.SECTION154 IS 'Section 154A Dwelling Units~~~D2074 - The number of dwelling units at an Eligible Premises that are eligible to receive Section 154A payments under the Water Industry Act 1991';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN MO_ELIGIBLE_PREMISES.UPRNREASONCODE IS 'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
COMMENT ON TABLE MO_SUPPLY_POINT IS 'Supply Point---The  provision of a water or sewer service at an Eligible Premise for a period of time. Each Eligible Premise has either one or two Supply Points at any point in time (no more than two); i.e. it can have either a water Supply Point, a sewerage Supply Point or both. The Supply Point is the tradeable item within the competitive water market in England and Wales; i.e. it is the level at which the end consumer can choose their retailer and therefore over time can be switched between retailers. The water and sewerage Supply Points at an Eligible Premise are independent in regards to both retailers and wholesalers; i.e. at any point in time the water and sewerage Supply Points at an Eligible Premise can have different Wholesalers and Retailers. ';
COMMENT ON COLUMN MO_SUPPLY_POINT.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN MO_SUPPLY_POINT.STWPROPERTYNUMBER_PK IS 'Target Property Number~~~STW014 Internal key to enable joins - STW020 - Property Number';
COMMENT ON COLUMN MO_SUPPLY_POINT.CORESPID_PK IS '~~~STW014 Internal key to enable joins - MO_ELIGIBLE_PREMISES.CORESPID_PK';
COMMENT ON COLUMN MO_SUPPLY_POINT.RETAILERID_PK IS '~~~STW014 Internal key to enable joins - MO_RETAILER_REGISTRATION.RETAILERID_PK';
COMMENT ON COLUMN MO_SUPPLY_POINT.WHOLESALERID_PK IS '~~~STW014 Internal key to enable joins - MO_ORG.ORGID_PK';
COMMENT ON COLUMN MO_SUPPLY_POINT.CUSTOMERNUMBER_PK IS '~~~STW017 - Legal Entity Number Unique identifier for the CUSTOMER';
COMMENT ON COLUMN MO_SUPPLY_POINT.SAPFLOCNUMBER IS 'SAP Floc number ~~~STW021 - SAP FLOC Number';
COMMENT ON COLUMN MO_SUPPLY_POINT.SERVICECATEGORY IS 'Service Category~~~D2002 - Identifies the Service Category for a Supply Point (Water Services or Sewerage Services) “W” = Water and “S” = Sewerage';
COMMENT ON COLUMN MO_SUPPLY_POINT.SUPPLYPOINTEFFECTIVEFROMDATE IS 'Supply Point Effective From Date~~~D2013 - Date that the Supply Point takes effect in the Central Systems.  This can be the date the connection was completed for newly established Supply Points, or the date that a Gap Site of Entry Change of Use takes effect.';
COMMENT ON COLUMN MO_SUPPLY_POINT.REGISTRATIONSTARTDATE IS 'Registration Start Date~~~D4002 - Date SPID becomes registered to a Retailer';
COMMENT ON COLUMN MO_SUPPLY_POINT.DISCONRECONDEREGSTATUS IS 'Disconnection/Reconnection/Deregistration~~~D2025 - Declares a Supply Point Disconnection, Reconnection or Deregistration. Also enables the distinction between a Temporary Disconnection and a Permanent Disconnection. Must contain “TDISC” if Supply Point is temporarily disconnected, otherwise must be unpopulated';
COMMENT ON COLUMN MO_SUPPLY_POINT.OTHERSERVICECATPROVIDED IS 'Other Service Category Provided Flag~~~D2041 - Flag indicating if services or no services provided of the other Service Category compared to a SPID at an Eligible Premises. May be required if we know of a SPID without supplies';
COMMENT ON COLUMN MO_SUPPLY_POINT.OTHERSERVICECATPROVIDEDREASON IS 'Other Service Category Provided Flag Reason~~~D2042 - Reason to explain the value of the Other Service Category Provided Flag. May be required if we know of a SPID without supplies';
COMMENT ON COLUMN MO_SUPPLY_POINT.MULTIPLEWHOLESALERFLAG IS 'Multiple Wholesalers Flag~~~D2053 - Boolean flag to indicate that there are multiple wholesalers for the same category of SPID at one site (where only the lead wholesaler is identified in the market and associated with the SPID)';
COMMENT ON COLUMN MO_SUPPLY_POINT.LANDLORDSPID IS 'Landlord SPID~~~D2070 - Identifies the Landlord Supply point in a multi-occupancy Eligible Premises.  The valid set for this is all SPIDs';
COMMENT ON COLUMN MO_SUPPLY_POINT.SPIDSTATUS IS 'SPID Status~~~D2088 - The logical status of a SPID';
COMMENT ON COLUMN MO_SUPPLY_POINT.NEWCONNECTIONTYPE IS 'New Connection Type~~~D2023 - Identifies the type of connection for a new Supply Point';
COMMENT ON COLUMN MO_SUPPLY_POINT.ACCREDITEDENTITYFLAG IS 'Accredited Entity Flag~~~D2033 - Declares whether the work being notified to the Market Operator was carried out by an Accredited Entity';
COMMENT ON COLUMN MO_SUPPLY_POINT.GAPSITEALLOCATIONMETHOD IS 'Gap Site Allocation Method~~~D2034 - Identifies how the Market Operator has allocated a Gap Site to a Retailer';
COMMENT ON COLUMN MO_SUPPLY_POINT.OTHERSPID IS 'Other SPID~~~D2091 - Unique identifier for second supply point where required in a transaction';
COMMENT ON COLUMN MO_SUPPLY_POINT.OTHERWHOLESALERID IS 'Other Wholesaler ID~~~D4018 - Unique ID identifying the Other Wholesaler i.e. the wholesale provider of services of the other Service Category at an Eligible Premises.';
COMMENT ON COLUMN MO_SUPPLY_POINT.PAIRINGREFREASONCODE IS 'Pairing Reference Reason Code~~~D2086 - Reason code for the absence of a pairing reference when requesting a new SPID. Must be populated with “NOSPID” if there is no pair for this Supply Point, otherwise it must be unpopulated';
COMMENT ON COLUMN MO_SUPPLY_POINT.LATEREGAPPLICATION IS 'Late Partial Registration Application~~~D2089 - Flag indicating when a SPID is awaiting a partial registration application but it has not been received.';
COMMENT ON COLUMN MO_SUPPLY_POINT.VOLTRANSFERFLAG IS 'Volume Transfer Flag~~~D2052 - Indicates when a SPID is included in a Volume Transfer process';
COMMENT ON COLUMN MO_SUPPLY_POINT.SUPPLYPOINTREFERENCE IS 'Supply Point Reference~~~STW019 - Used to identity supply point whilst awaiting official SPID from Market Operator';
COMMENT ON TABLE MO_ORG IS 'Organisation---A participant in the competitive water market in England and Wales. There are currently five types: Wholesalers, Retailers (both otherwise known as  Trading Parties), External Auditors, Regulators and the Central Market Operator (MOSL) - though this could increase over time as the market evolves.';
COMMENT ON COLUMN MO_ORG.ORGID_PK IS 'Organisation ID~~~STW022 - Organization ID';
COMMENT ON COLUMN MO_ORG.ORGNAME IS 'Organisation Name~~~D4013 - The organisation name of an organisation which has a Trading Party ID';
COMMENT ON COLUMN MO_ORG.ORGTYPE IS 'Organisation Type~~~STW023 - Organization Type';
COMMENT ON COLUMN MO_ORG.RETAILERTYPE IS 'Retailer Type~~~D4014 - This identifies what form of Licence a Retailer possesses';
COMMENT ON COLUMN MO_ORG.INTERIMSUPPLIERALLOCSTATUS IS 'Interim Supplier Allocation Status~~~D4015 - Identifies whether a Retailer has opted in to the Interim Supplier Allocation Process';
COMMENT ON COLUMN MO_ORG.TRADINGPARTYSERVICECAT IS 'Trading Party Service Categories~~~D4016 - Identifies whether the Retailer has a License and if so, for what Service Category';
COMMENT ON COLUMN MO_ORG.GAPSITEALLOCSTATUS IS 'Gap Site Allocation Status~~~D4017 - Identifies if a Retailer is opted in to the Gap Site allocation process';
COMMENT ON COLUMN MO_ORG.DEFAULTRETURNTOSEWER IS 'Default Return To Sewer~~~D7051 - The default value for the percentage Volume which is deemed to return to sewer. This is used where sewerage charges are calculated from metered potable, metered non-potable and private water supplies. Where there is a variation from the default value applied to metered volumes, this should be adjusted for the relevant meter on a case by case basis by the Wholesaler in accordance with CSD 0104 (Maintain SPID Data). This value is also used where sewerage and Trade Effluent charges are calculated based on Domestic Allowance i.e. the Return to Sewer percentage is applied to the Domestic Allowance, and this cannot be varied on a case by case basis';
COMMENT ON COLUMN MO_ORG.VACANCYCHARGINGMETHODWATER IS 'Vacancy Charging Method Water~~~D7052 - The Wholesalers selected method for charging for Water Services where an Eligible Premises is vacant';
COMMENT ON COLUMN MO_ORG.VACANCYCHARGINGMETHODSEWERAGE IS 'Vacancy Charging Method Sewerage~~~D7053 - The Wholesalers selected method for charging for Sewerage Services where an Eligible Premises is vacant';
COMMENT ON COLUMN MO_ORG.TMPDISCONCHRGMETHODWATER IS 'Temporary Disconnection Charging Method Water~~~D7054 - The Wholesalers selected method for charging for Water Services where an Eligible Premises is temporarily disconnected';
COMMENT ON COLUMN MO_ORG.TMPDISCONCHRGMETHODSEWERAGE IS 'Temporary Disconnection Charging Method Sewerage~~~D7055 - The Wholesalers selected method for charging for Sewerage Services where an Eligible Premises is temporarily disconnected';
COMMENT ON TABLE MO_RETAILER_REGISTRATION IS 'Registration---A record of which Retailer has, is or will be (assuming their transfer request is not rejected) supplying the end consumer with services at a Supply Point. It contains historical, current, planned rejected and erroneous registrations for the allocation/transfer of Supply Points between Retailers. ';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.RETAILERID_PK IS 'Retailer ID~~~D4011 - The Trading Party ID of a Retailer';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.REGISTRATIONSTARTDATE IS 'Registration Start Date~~~D4002 - Date SPID becomes registered to a Retailer';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERSTATUS IS 'Transfer Status~~~STW024 - The current status of the transfer; e.g. pending, live, cancelled or erroneous';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.REGISTRATIONTYPE IS 'Registration Type~~~STW025 - What was the trigger for this registration; i.e. Customer Selected, Bulk Transfer, Gap Site Allocation or Interim Supplier';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANFERREQUESTTIMESTAMP IS 'Transfer Request Timestamp~~~STW026 - To state the date and time a transfer was initiated. ';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERREADREQUIREDBYDATE IS 'Transfer Read Required By Date~~~STW027 - The date by which a transfer read should be submitted';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.CANCELLATIONCODE IS 'Cancellation Code~~~D4005 - Used by Retailers to specify cancellation reason';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERCANCELREASON IS 'Transfer Cancellation Reason~~~STW028 - Hold details at to why the system automatically stopped the transfer during its validation of application processes; i.e. as opposed to D4005 which is a TP supplied reason.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.DATEOFINVOICEINDISPUTE IS 'Date of invoice in dispute~~~D4019 - Date of invoice in dispute where a transfer has been cancelled due to outstanding debt.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TOTALAMOUNTOUTSTANDING IS 'Total outstanding amount on account~~~D4020 - Total outstanding amount on account where a transfer has been cancelled due to outstanding debt.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.ORIGINALDUEDATEOFINVOICE IS 'Original due date of invoice~~~D4021 - Original due date of invoice where a transfer has been cancelled due to outstanding debt.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.DATEREMINDERSENT IS 'Date reminder sent~~~D4022 - Date reminder sent where a transfer has been cancelled due to outstanding debt.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.NEWDUEDATESETINREMINDER IS 'New due date specified in reminder~~~D4023 - New due date specified in reminder where a transfer has been cancelled due to outstanding debt.';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERAPPLICATIONREASON IS 'Erroneous Transfer Application Reason~~~D2054 - When a new retailer confirms that they should not have had the SPID transferred to them, they must provide a reason';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERVALIDFLAG IS 'Erroneous Transfer Valid Flag~~~D2055 - Flag for the previous retailer to confirm that they should not have had the SPID transferred from them';
COMMENT ON COLUMN MO_RETAILER_REGISTRATION.TRANSFERREJECTIONREASON IS 'Erroneous Transfer Rejection Reason~~~D2075 - Identifies the reason why the outgoing Retailer has rejected the erroneous transfer application from the Incoming Retailer';
COMMENT ON TABLE MO_SERVICE_COMPONENT IS 'Service Component---The actual provision of a type of Service Component of a specific type at a Supply Point. The Service Components for a Supply Point can vary over time. Each Supply Point cant have more than one Service Component of a specific type; e.g. a Water Supply Point could only have either none or one Unmeasured Water Service Component – it couldnt have two or more. In terms of Trade Effluent it still can only have one Trade Effluent Service Component, but this can have many Discharge Points.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SERVICECOMPONENTREF_PK IS 'Service Compenet Ref~~~STW029 -  Service ID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.TARIFFCODE_PK IS '~~~';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.STWSERVICETYPE IS 'Service Type~~~STW030 - Type of Service Required';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SERVICECOMPONENTTYPE IS 'Service Component~~~D2043 - Service Component of a water or sewerage SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SERVICECOMPONENTENABLED IS 'Service Component Enabled~~~D2076 - Identifies if the Service Component is switched on or off';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SPECIALAGREEMENTFACTOR IS 'Special Agreement Factor~~~D2003 - Percentage factor applied to a Service Component or a DPID where a Special Arrangement exists. When set to zero it results in a zero charge, when set to 100% no adjustment is applied, and can be set to > 100%';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SPECIALAGREEMENTFLAG IS 'Special Agreement Flag~~~D2004 - Identifies the presence of a Special Agreement at a Service Component or a DPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SPECIALAGREEMENTREF IS 'Special Agreement Reference~~~D2090 - Ofwat Reference for any S.142(2)(b) Special Agreement in place for the Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDFSMAXDAILYDEMAND IS 'Tariff Code Applicable~~~D2080 - Daily reserved capacity in m3 for a Metered Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDPWMAXDAILYDEMAND IS 'Maximum Daily Demand~~~D2079 - Maximum daily demand in m3 for a Metered Service Component, for maximum demand tariffs';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDNPWMAXDAILYDEMAND IS 'Maximum Daily Demand~~~D2079 - Maximum daily demand in m3 for a Metered Service Component, for maximum demand tariffs';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDFSDAILYRESVDCAPACITY IS 'Daily Reserved Capacity~~~D2080 - Daily reserved capacity in m3 for a Metered Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDNPWDAILYRESVDCAPACITY IS 'Daily Reserved Capacity~~~D2080 - Daily reserved capacity in m3 for a Metered Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDFSTARIFFCODE IS 'Metered Foul Sewerage Tariff Code~~~D2063 - Indicates the tariff code for this Service Component to be applied at the Supply Point. The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDNPWTARIFFCODE IS 'Metered Non-Potable Water Tariff Code~~~D2057 - Indicates the tariff code for this Service Component to be applied at the Supply Point. The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.DAILYRESERVEDCAPACITY IS 'Daily Reserved Capacity~~~D2080 - Daily reserved capacity in m3 for a Metered Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDPWTARIFFCODE IS 'Metered Potable Water Tariff Code~~~D2056 - Indicates the tariff code for this Service Component to be applied at the Supply Point. The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.HWAYSURFACEAREA IS 'Surface Area~~~D2012 - Indicates the Surface area in m2 of Eligible Premises for Highway Drainage calculations';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.HWAYDRAINAGETARIFFCODE IS 'Highway Drainage Tariff Code~~~D2017 - Indicates the tariff code for this Service Component to be applied at the Supply Point. The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.HWAYCOMMUNITYCONFLAG IS 'Community Concession Flag~~~D2085 - Boolean Flag indicating if Community Concession is to be applied to a Surface Water or Highway Drainage Service Component.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.ASSESSEDDVOLUMETRICRATE IS 'Assessed Volumetric Rate~~~D2049 - The Volume (in m3 per year) to be used in charge calculations for Assessed Service Components';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.ASSESSEDCHARGEMETERSIZE IS 'Assessed Chargeable Meter Size~~~D2068 - Meter size in mm for charge calculation purposes, for Assessed Service Components';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.ASSESSEDTARIFFCODE IS 'Assessed Tariff Code~~~D2066 - Indicates the tariff code for this Service Component to be applied at the Supply Point. The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.ASSESSEDTARIFBAND IS 'Tariff band~~~D2081 - Tariff band number to be applied for Service Components where banded tariffs are permitted';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SRFCWATERAREADRAINED IS 'Area drained~~~D2078 - Area drained at the Eligible Premises in m2, for the purposes of calculating Surface Water drainage charges';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SRFCWATERTARRIFCODE IS 'Surface Water Tariff Code~~~D2016 - Indicates the tariff code for this Service Component to be applied at the Supply Point.  The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.SRFCWATERCOMMUNITYCONFLAG IS 'Community Concession Flag~~~D2085 - Boolean Flag indicating if Community Concession is to be applied to a Surface Water or Highway Drainage Service Component.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEACOUNT IS 'Unmeasured Items Type A Count~~~D2018 - Indicates how many of Unmeasured Items Type A are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEBCOUNT IS 'Unmeasured Items Type B Count~~~D2019 - Indicates how many of Unmeasured Items Type B are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPECCOUNT IS 'Unmeasured Items Type C Count~~~D2020 - Indicates how many of Unmeasured Items Type C are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEDCOUNT IS 'Unmeasured Items Type D Count~~~D2021 - Indicates how many of Unmeasured Items Type D are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEECOUNT IS 'Unmeasured Items Type E Count~~~D2022 - Indicates how many of Unmeasured Items Type E are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEFCOUNT IS 'Unmeasured Items Type F Count~~~D2024 - Indicates how many of Unmeasured Items Type F are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEGCOUNT IS 'Unmeasured Items Type G Count~~~D2046 - Indicates how many of Unmeasured Items Type G are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEHCOUNT IS 'Unmeasured Items Type H Count~~~D2048 - Indicates how many of Unmeasured Items Type H are present at the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEADESCRIPTION IS 'Unmeasured Items Type A Description~~~D2058 - Free text description of the Unmeasured Items of Type A applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEBDESCRIPTION IS 'Unmeasured Items Type B Description~~~D2059 - Free text description of the Unmeasured Items of Type B applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPECDESCRIPTION IS 'Unmeasured Items Type C Description~~~D2060 - Free text description of the Unmeasured Items of Type C applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEDDESCRIPTION IS 'Unmeasured Items Type D Description~~~D2061 - Free text description of the Unmeasured Items of Type D applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEEDESCRIPTION IS 'Unmeasured Items Type E Description~~~D2062 - Free text description of the Unmeasured Items of Type E applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEFDESCRIPTION IS 'Unmeasured Items Type F Description~~~D2064 - Free text description of the Unmeasured Items of Type F applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEGDESCRIPTION IS 'Unmeasured Items Type G Description~~~D2065 - Free text description of the Unmeasured Items of Type F applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTYPEHDESCRIPTION IS 'Unmeasured Items Type H Description~~~D2069 - Free text description of the Unmeasured Items of Type H applied to the SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT.UNMEASUREDTARIFFCODE IS 'Unmeasured Tariff Code~~~D2067 - Indicates the tariff code for this Service Component to be applied at the Supply Point.  The valid set for this is all Tariff Codes associated with the relevant Service Component';
COMMENT ON TABLE MO_SERVICE_COMPONENT_TYPE IS 'Service Component Type.     A sub-category of service supplied at the Supply Point.  Each Supply Point has at least one and possibly multiple Service Components.  The Service Components at any Supply Point can have vary depending on whether it is a water or sewerage Supply Point. A water Supply Point can have Service Components of Metered Potable Water, Metered Non-Potable Water, Assessed Water, Unmeasured Water and Water Charge Adjustment. It cannot have the last of these on its own; i.e. there must be at least one other Service Component for the Supply Point. A sewerage Supply Point can have Service Components of Metered Sewerage, Assessed Sewerage, Unmeasured Sewerage, Surface Water, Highway Drainage, Trade Effluent and Sewerage Charge Adjustment. It cannot have the last of these on its own; i.e. there must be at least one other Service Component for the Supply Point. ';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_TYPE.SERVICECOMPONENTTYPE IS 'Service Component~~~D2043 - Service Component of a water or sewerage SPID';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_TYPE.SERVICECOMPXREF IS 'Service Component X-Ref~~~STW032 - A description of the Service Component Type';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_TYPE.VACANCYTEMPDISCONNPARAM IS 'Vacancy and Temporary Disconnection Parameters~~~';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_TYPE.CHARGINGELEMENT IS 'Charging Element~~~';
COMMENT ON TABLE MO_SERVICE_COMPONENT_VOL_ADJ IS 'Service Component Volumetric Adjustment---A volumetric adjustment that needs to be applied to a Service Component.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.ADJUSTMENTSVOLADJUNIQREF_PK IS 'Volumetric Adjustment Unique Reference~~~D2045 - The unique reference of the volumetric adjustment to be applied to the Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.SERVICECOMPONENTREF_PK IS '~~~STW014-Internal key to enable joins - MO_SERVICE_COMPONENT.SERVICECOMPONENTREF_PK';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.ADJUSTMENTSVOLADJTYPE IS 'Volumetric Adjustment Type~~~D2044 - The type of volumetric adjustment to be applied to the Service Component';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.ADJUSTMENTSVOLUME IS 'Adjustment Volume~~~D2047 - The signed volume of the adjustment in m3. This should be positive for cases where the meter is under-recording, and negative to give allowances for firefighting, bursts, etc.';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.ADJUSTMENTSGOVTCONTRIB IS 'Government Contribution~~~STW - Need more information on this, cant find anything in the source documentation';
COMMENT ON COLUMN MO_SERVICE_COMPONENT_VOL_ADJ.ADJUSTMENTSCHARGEADJTARIFFCODE IS 'Charge Adjustment Tariff Code~~~D2051 - Indicates the tariff code for this Service Component to be applied at the Supply Point';
COMMENT ON TABLE MO_DISCHARGE_POINT IS 'Discharge Point---If a sewerage Supply Point has a Trade Effluent Service Component then it should have at least one and possibly many Discharge Points associated with the Trade Effluent Service Component. These can vary over time. A Discharge Point is a distinct point from which trade effluent sewerage is discharged from the Eligible Premise into the sewer network. ';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler.';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator. STW001 - FK implementing relationship to Trade Effluent Service Component of SPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SERVICECOMPTYPE IS 'Service Component Type~~~D2043 - Service Component of a water or sewerage SPID. STW001 - FK implementing relationship to Trade Effluent Service Component of SPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SCEFFECTIVEFROMDATE IS 'SC Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW001 - FK implementing relationship to Trade Effluent Service Component of SPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPEFFECTFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPEFFECTTODATE IS 'DP Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is tThe date that new data or any change to data included in the Data Transaction is effective to';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DISCHARGEPOINTERASEFLAG IS 'Discharge Point Erased Flag~~~STW002 - Flag to show that Discharge Point has been logically deleted as part of an erase corrective transaction';
COMMENT ON COLUMN MO_DISCHARGE_POINT.EFFECTFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW001 - FK implementing relationship to Trade Effluent Service Component of SPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.EFFECTTODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is tThe date that new data or any change to data included in the Data Transaction is effective to';
COMMENT ON COLUMN MO_DISCHARGE_POINT.WHOLESALERID IS 'Wholesaler ID~~~D4025 - Unique ID identifying the Wholesaler. STW003 - FK implementing relationship to Tariff and Tariff Band (if required).';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TARRIFCODE IS 'Tariff Code~~~D7001 - A short code specified by the Wholesaler for the Tariff. The Tariff Code should be meaningful. This code is unique to the Wholesaler for the Service Component and can never be changed. STW004 - FK implementing relationship to Tariff and (if used) the Tariff Band. Covers off D6020';
COMMENT ON COLUMN MO_DISCHARGE_POINT.CHARGEABLEDAILYVOL IS 'Chargeable Daily Volume~~~D6003 - Trade Effluent Availability Data: chargeable daily volume in m3/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.AMMONIANITROCAL IS 'cANℓ~~~D6002 - Trade Effluent Availability Data: chargeable ammoniacal nitrogen load in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.CHEMICALOXYGENDEMAND IS 'cCODℓ~~~D6004 - Trade Effluent Availability Data: chargeable chemical oxygen demand load (or other parameter as may be determined by the Wholesaler) in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SUSPENDEDSOLIDSLOAD IS 'cSSℓ~~~D6005 - Trade Effluent Availability Data: chargeable suspended solids load (or other parameter as may be determined by the Wholesaler) in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DOMMESTICALLOWANCE IS 'Domestic Allowance~~~D6009 - The annual Volume in m3 of Water Services in relation to water meters associated with a Discharge Point that is being used for domestic purposes and is discharged as Trade Effluent Services in relation to the Discharge Point. If the Sewerage Volume Adjustment Method is set to “DA”, then the Domestic Allowance must be specified. If the Sewerage Volume Adjustment Method is set to either “NONE” or “SUBTRACT”, then the Domestic Allowance must not be specified. For the avoidance of doubt, if a Domestic Allowance of zero (0) is specified, this will result in a zero volume being applied to the Foul Sewerage calculation in respect of the applicable meters';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SEASONALFACTOR IS 'Seasonal Factor~~~D6010 - Trade Effluent Availability Data: premium to the Trade Effluent Charges in accordance with the Wholesale Tariff Document';
COMMENT ON COLUMN MO_DISCHARGE_POINT.PERCENTAGEALLOWANCE IS 'Percentage Allowance~~~D6012 - The part of the Volume of Water Services of all meters associated with the Discharge Point which is expressed as a percentage and is not discharged to the Sewerage Wholesaler’s sewer, for example due to evaporation or because it is used in production. The Percentage Allowance is applied after the Fixed Allowance. The Percentage Allowance must be supplied';
COMMENT ON COLUMN MO_DISCHARGE_POINT.FIXEDALLOWANCE IS 'Fixed Allowance~~~D6013 - The part of the Volume of Water Services of all meters associated with the Discharge Point which is expressed as an annual Volume in m3 and is not discharged to the Sewerage Wholesaler’s sewer; for example due to evaporation, because it is used in production or because it is Surface Water recorded by a Private Trade Effluent Meter in respect of which Surface Water Drainage Service charges are already applied. The Fixed Allowance is applied before the Percentage Allowance. The Fixed Allowance must be supplied';
COMMENT ON COLUMN MO_DISCHARGE_POINT.RECEPTIONTREATMENTINDICATOR IS 'Reception Treatment Indicator~~~D6014 - Flag to indicate whether Reception Charges apply to Trade Effluent from the Discharge Point Variable name: RTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.PRIMARYTREATMENTINDICATOR IS 'Primary Treatment Indicator~~~D6015 - Flag to indicate Primary/Volumetric Charges apply to Trade Effluent from the Discharge Point Variable name: PTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.MARINETREATMENTINDICATOR IS 'Marine Treatment Indicator~~~D6016 - Flag to indicate whether Outfall (marine) Charges apply to Trade Effluent from the Discharge Point Variable name: MTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.BIOLOGICALTREATMENTINDICATOR IS 'Biological Treatment Indicator~~~D6017 - Flag to indicate whether Secondary (Biological) Charges apply to Trade Effluent from the Discharge Point Variable name: BTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SLUDGETREATMENTINDICATOR IS 'Sludge Treatment Indicator~~~D6018 - Flag to indicate whether Sludge Treatment Charges apply to Trade Effluent from the Discharge Point Variable name: STI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.AMMONIATREATMENTINDICATOR IS 'Ammonia Treatment Indicator~~~D6019 - Flag to indicate whether ammonia charges apply to Trade Effluent from the Discharge Point Variable name: ATI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFXTREATMENTINDICATOR IS 'Trade Effluent Component X Treatment Indicator~~~D6029 - Flag to indicate whether Trade Effluent Component X applies at the Discharge Point. Variable name: XTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFYTREATMENTINDICATOR IS 'Trade Effluent Component Y Treatment Indicator~~~D6030 - Flag to indicate whether Trade Effluent Component Y applies at the Discharge Point. Variable name: YTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFZTREATMENTINDICATOR IS 'Trade Effluent Component Z Treatment Indicator~~~D6031 - Flag to indicate whether Trade Effluent Component Z applies at the Discharge Point Variable name: ZTI';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFAVAILABILITYDATAX IS 'cXℓ~~~D6032 - Trade Effluent Availability Data: Trade Effluent Component X Demand load in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFAVAILABILITYDATAY IS 'cYℓ~~~D6033 - Trade Effluent Availability Data: Trade Effluent Component Y Demand load in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TEFAVAILABILITYDATAZ IS 'cZℓ~~~D6034 - Trade Effluent Availability Data: Trade Effluent Component Z Demand load in kg/day';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TARRIFBAND IS 'Tariff Band~~~D2081 - Tariff band number to be applied for Service Components where banded tariffs are permitted. Covers off D6024';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SEWERAGEVOLUMEADJMENTHOD IS 'Sewerage Volume Adjustment Method~~~D6035 - The method by which Sewerage volumes are adjusted, if required, due to the DPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.SECONDADDRESSABLEOBJ IS 'Secondary Addressable Object~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN MO_DISCHARGE_POINT.PRIMARYADDRESSABLEOBJ IS 'Primary Addressable Object~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMOXYGENDEMAND IS 'Ot~~~D6006 - Trade Effluent Operating Data: chemical oxygen demand (or other parameter as may be determined by the Wholesaler) in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMSUSPSOLDEMAND IS 'St~~~D6007 - Trade Effluent Operating Data: suspended solids (or other parameter as may be determined by the Wholesaler) in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMAMONIANITROGENDEMAND IS 'At~~~D6011 - Trade Effluent Operating Data: ammoniacal nitrogen content of the Trade Effluent in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMCOMPXDEMAND IS 'Xt~~~D6026 - Trade Effluent Operating Data: Trade Effluent Component X content in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMCOMPYDEMAND IS 'Yt~~~D6027 - Trade Effluent Operating Data: Trade Effluent Component Y content in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.TREFODCHEMCOMPZDEMAND IS 'Zt~~~D6028 - Trade Effluent Operating Data: Trade Effluent Component Z content in mg/l';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPIDSPECIALAGREEMENTINPLACE IS 'Special Agreement Flag~~~D2004 - Identifies the presence of a Special Agreement at a Service Component or a DPID';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPIDSPECIALAGREEMENTFACTOR IS 'Special Agreement Factor~~~D2003 - Percentage factor applied to a Service Component or a DPID where a Special Arrangement exists. When set to zero it results in a zero charge, when set to 100% no adjustment is applied, and can be set to > 100%';
COMMENT ON COLUMN MO_DISCHARGE_POINT.DPIDSPECIALAGREEMENTREFERENCE IS 'Special Agreement Reference~~~D2090 - Ofwat Reference for any S.142(2)(b) Special Agreement in place for the Service Component';
COMMENT ON TABLE MO_CALCULATED_DISCHARGE IS 'Calculated Discharge---A Discharge of a particular type that is calculated for a Discharge Point for Settlement purposes.';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.CALCDISCHARGEID_PK IS 'Calculated Discharge ID~~~D6023 - Unique reference for the discharge. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.DPEFFECTFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.EFFECTFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from.';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.EFFECTTODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective to.';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.DISCHARGETYPE IS 'Discharge Type~~~D6022 -  Discharge type. ';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.SUBMISSIONFREQ IS 'Submission Frequency~~~D6025 - The frequency that the Retailer must submit Calculated Discharges';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.TEYEARLYVOLESTIMATE IS 'TE Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';
COMMENT ON TABLE MO_DISCHARGED_VOLUME IS 'Discharged Volume---Submission of Trade Effluent calculated volume for Settlement calculations. Each notification should be for the volume measured since the previous submission.';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.DISCHARGEVOLUME_PK IS '~~~';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.DPEFFECTFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.EFFECTFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from.';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.EFFECTTODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective to.';
COMMENT ON COLUMN MO_DISCHARGED_VOLUME.NOTIFIEDVOLUME IS 'Notified Volume~~~D6008 - Volume in m3 notified as having been discharged for a Calculated Discharge';
COMMENT ON TABLE MO_DISCHARGE_POINT_VOLMET_ADJ IS 'Discharge Point Volumetric Adjustment---A volumetric adjustment that needs to be applied to a Discharge Point.';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.DISCHARGEPOINTVOLMETADJ_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.DPEFFECTFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.EFFECTFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from.';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.EFFECTTODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective to.';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.ADJUSTMENTSVOLADJTYPE IS 'Volumetric Adjustment Type~~~D2044 - The type of volumetric adjustment to be applied to the Service Component';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.ADJUSTMENTSVOLADJUNIQREF IS 'Volumetric Adjustment Unique Reference~~~D2045 - The unique reference of the volumetric adjustment to be applied to the Service Component';
COMMENT ON COLUMN MO_DISCHARGE_POINT_VOLMET_ADJ.ADJUSTMENTSVOLUME IS 'Adjustment Volume~~~D2047 - The signed volume of the adjustment in m3. This should be positive for cases where the meter is under- recording, and negative to give allowances for firefighting, bursts, etc.';
COMMENT ON TABLE MO_METER IS 'Meter---A physical device used to measure water or sewerage.';
COMMENT ON COLUMN MO_METER.MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER.MANUFACTURERSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s serial number of a meter. STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN MO_METER.INITIALMETERREADDATE IS 'Initial Meter Read Date~~~D3042 - Date of Initial meter read, used only to uniquely identify the meter in data correction transactions';
COMMENT ON COLUMN MO_METER.METERERASEDFLAG IS 'Meter Erased Flag~~~STW033 - Flag to show if meter has been logically deleted';
COMMENT ON COLUMN MO_METER.NONMARKETMETERFLAG IS 'Non-market Meter Flag~~~STW034 - Flag to show that the Meter is a non-market meter';
COMMENT ON COLUMN MO_METER.WHOLESALERID IS 'Wholesaler ID~~~D4025 - Unique ID identifying the Wholesaler';
COMMENT ON COLUMN MO_METER.INSTALLEDBYACCREDITEDENTITY IS 'Installed By Accredited Entity~~~D2033 - Declares whether the work being notified to the Market Operator was carried out by an Accredited Entity';
COMMENT ON COLUMN MO_METER.NUMBEROFDIGITS IS 'Number of Digits~~~D3004 - The number of digits required to provide a reading in m3. For the avoidance of doubt, this is irrespective of the actual number of dials or digits on the meter, as meters may record volumes to a higher or lower resolution than 1m3. However, this Data Item is required with reference to m3 for the purposes of rollover detection. This will also be the number of digits required for the maximum volume in m3 that can be recorded by the meter';
COMMENT ON COLUMN MO_METER.METERTREATMENT IS 'Meter Treatment~~~D3022 - Specifies whether the meter is a Wholesaler Water Meter or one of the various types of Private Meter';
COMMENT ON COLUMN MO_METER.MEASUREUNITFREEDESCRIPTOR IS 'Measurement Units Free Descriptor~~~D3035 - Free descriptor to provide further information on the measurements  units of the meter e.g. if the meter is a x10 meter';
COMMENT ON COLUMN MO_METER.MEASUREUNITFREEATMETER IS 'Measurement Units at Meter~~~D3036 - Indicates the measurement units of the meter itself';
COMMENT ON COLUMN MO_METER.METERADDITIONREASON IS 'Meter Addition Reason~~~D3045 - Reason for the addition of a new meter';
COMMENT ON COLUMN MO_METER.METERREMOVALREASON IS 'Meter Removal Reason~~~D3046 - Reason for the removal of a meter';
COMMENT ON COLUMN MO_METER.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_METER.EFFECTIVETODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is tThe date that new data or any change to data included in the Data Transaction is effective to';
COMMENT ON COLUMN MO_METER.METERREADMINFREQUENCY IS 'Meter Read Minimum Frequency~~~D3011 - The minimum frequency with which the Retailer must read a meter';
COMMENT ON COLUMN MO_METER.PHYSICALMETERSIZE IS 'Physical Meter Size~~~D3003 - Nominal size of the meter in mm e.g. for a DN15 meter the Physical Meter Size is 15';
COMMENT ON COLUMN MO_METER.DATALOGGERWHOLESALER IS 'Datalogger (Wholesaler)~~~D3015 - Specifies the presence of a Wholesaler datalogger';
COMMENT ON COLUMN MO_METER.DATALOGGERNONWHOLESALER IS 'Datalogger (Non-Wholesaler)~~~D3016 - Specifies the presence of a non-Wholesaler datalogger';
COMMENT ON COLUMN MO_METER.GPSX IS 'GISX~~~D3017 - Specifies the X coordinate of the location of the meter, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN MO_METER.GPSY IS 'GISY~~~D3018 - Specifies the Y coordinate of the location of the meter, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN MO_METER.METERLOCATIONCODE IS 'Meter Location Code~~~D3025 - Indicates Meter Location as either inside or outside (of a building)';
COMMENT ON COLUMN MO_METER.METEROUTREADERGPSX IS 'Meter Outreader GISX~~~D3030 - Specifies the X coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN MO_METER.METEROUTREADERGPSY IS 'Meter Outreader GISY~~~D3031 - Specifies the Y coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN MO_METER.METEROUTREADERLOCCODE IS 'Meter Outreader Location Code~~~D3033 - Indicates Meter Outreader Location as either inside or outside (of a building)';
COMMENT ON COLUMN MO_METER.COMBIMETERFLAG IS 'Combi Meter Flag~~~D3034 - Indicates if meter is part of a combi meter. Each part of a combi meter must have this flag set.';
COMMENT ON COLUMN MO_METER.REMOTEREADFLAG IS 'Remote Read Flag~~~D3037 - Indicates if a meter has the capability to be read remotely, including via an Outreader';
COMMENT ON COLUMN MO_METER.REMOTEREADTYPE IS 'Remote Read Type~~~D3038 - Indicates the type of remote read capability for the meter';
COMMENT ON COLUMN MO_METER.OUTREADERID IS 'Outreader ID~~~D3039 - Free text Data Item for the encoder reference, radio ID or logger number, or any other reference which will assist the Retailer in reading the meter';
COMMENT ON COLUMN MO_METER.OUTREADERPROTOCOL IS 'Outreader Protocol~~~D3040 - Free text providing details of how the reading is accessed by the outreader. Will typically identify the manufacturers  protocol in use';
COMMENT ON COLUMN MO_METER.FREEDESCRIPTION IS 'Free Descriptor~~~D5001 - Free text descriptor for address/location  details';
COMMENT ON COLUMN MO_METER.SECADDRESSABLEOBJ IS 'Secondary Addressable Object~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN MO_METER.PRIMADDRESSABLEOBJ IS 'Primary Addressable Object~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN MO_METER.ADDRESSLINE01 IS 'Address Line 1~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_METER.ADDRESSLINE02 IS 'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_METER.ADDRESSLINE03 IS 'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_METER.ADDRESSLINE04 IS 'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_METER.ADDRESSLINE05 IS 'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_METER.POSTCODE IS 'Postcode~~~D5009 - Postcode (without spaces)';
COMMENT ON COLUMN MO_METER.COUNTRY IS 'Country~~~D5010 - Country, if address is outside the UK';
COMMENT ON COLUMN MO_METER.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';
COMMENT ON COLUMN MO_METER.WATERCHARGEMETERSIZE IS 'Water Chargeable Meter Size~~~D3002 - Meter size for Water Services tariff charge calculation purposes in mm. In most cases this will equal physical meter size';
COMMENT ON COLUMN MO_METER.RETURNTOSEWER IS 'Return to Sewer~~~D3007 - The fraction of the volume which is deemed to return to sewer for a particular meter in %';
COMMENT ON COLUMN MO_METER.SEWCHARGEABLEMETERSIZE IS 'Sewerage Chargeable Meter Size~~~D3005 - Meter size for Foul Sewerage Services tariff charge calculation purposes in mm';
COMMENT ON COLUMN MO_METER.METERLOCFREEDESCRIPTOR IS 'Meter Location Free Descriptor~~~D3019 - Retailers Free Descriptor of the location of the meter.';
COMMENT ON COLUMN MO_METER.METEROUTREADLOCFREEDESCRIPTOR IS 'Meter Outreader Location Free Descriptor~~~D3032 - Retailers Free Descriptor for the location of the outreader. Can be provided by the Wholesaler on initial setup of the outreader.';
COMMENT ON COLUMN MO_METER.MAINMETERMANUFACTURER IS 'Main Meter Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter';
COMMENT ON COLUMN MO_METER.MAINMETERMANUFACTURERSERIALNUM IS 'Main Meter Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturers  serial number of a meter';
COMMENT ON COLUMN MO_METER.MAINMETERINITIALMETERREADDATE IS 'Main Meter Initial Meter Read Date~~~D3042 - Date of Initial meter read, used only to uniquely identify the meter in data correction transactions';
COMMENT ON COLUMN MO_METER.YEARLYVOLESTIMATE IS 'Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';
COMMENT ON TABLE MO_METER_READING IS 'Meter Reading---Either the actual or estimated value on a meters register at a certain date and time.';
COMMENT ON COLUMN MO_METER_READING.METER_READING_PK IS '~~~';
COMMENT ON COLUMN MO_METER_READING.MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason being that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_READING.MANUFACTURERSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s  serial number of a meter.  STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason being that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_READING.INITIALMETERREADDATE IS 'Initial Meter Read Date~~~D3042 - Date of Initial meter read, used only to uniquely identify the meter in data correction transactions';
COMMENT ON COLUMN MO_METER_READING.METERREADDATE IS 'Meter Read Date~~~D3009 - Date of meter read';
COMMENT ON COLUMN MO_METER_READING.METERREAD IS 'Meter Read~~~D3008 - Register advance read from a meter in m3';
COMMENT ON COLUMN MO_METER_READING.METERREADTYPE IS 'Meter Read Type~~~D3010 - The type of meter reading';
COMMENT ON COLUMN MO_METER_READING.METERREADMETHOD IS 'Meter Read Method~~~D3044 - The method of meter reading';
COMMENT ON COLUMN MO_METER_READING.REREADFLAG IS 'Re-Read Flag~~~D3012 - Identifies a meter read as a re-read';
COMMENT ON COLUMN MO_METER_READING.ROLLOVERINDICATOR IS 'Rollover Indicator~~~D3020 - Proposes whether the meter read has rolled over or not as part of meter read submission by the Retailer or  Wholesaler';
COMMENT ON COLUMN MO_METER_READING.ROLLOVERFLAG IS 'Rollover Flag~~~D3021 - Set by the Market Operator to indicate whether the Market Operator believes the meter read has rolled over or not';
COMMENT ON COLUMN MO_METER_READING.RDAOUTCOME IS 'RDA Outcome~~~STW005 - Holds the result of the rollover detection algorithm for this meter reading; for initial reads will hold value equivalent to Not a Rollover.';
COMMENT ON COLUMN MO_METER_READING.ESTIMATEDREADREASONCODE IS 'Estimated Read Reason Code~~~D3028 - Identifies the reason for use of a Transfer Read with Meter Read Method of “Estimated”';
COMMENT ON COLUMN MO_METER_READING.ESTIMATEDREADREMEDIALWORKIND IS 'Estimated Read Remedial Work Indicator~~~D3029 - Identifies whether remedial action has been obtained for a meter associated with a transfer, when a Transfer Read with Meter Read Method of “Estimated” is submitted';
COMMENT ON COLUMN MO_METER_READING.METERREADSTATUS IS 'Meter Reading Status~~~STW005 - Indicates whether reading has passed validation or if not why not.';
COMMENT ON COLUMN MO_METER_READING.METERREADERASEDFLAG IS 'Meter Reading Erased Flag~~~STW006 - Flag to show that Meter Reading has been logically deleted.';
COMMENT ON COLUMN MO_METER_READING.METERREADREASONTYPE IS 'Meter Reading Reason Type~~~STW007 - Type of reading; i.e. Standard Read, Replacement Read, Replaced Read, Inserted Read, Removed Read';
COMMENT ON COLUMN MO_METER_READING.METERREADSETTLEMENTFLAG IS 'Meter Reading Settlement Flag~~~STW008 - Flag show whether Meter Read can be used in the Settlement Process';
COMMENT ON COLUMN MO_METER_READING.PREVVALCDVCANDIDATEDAILYVOLUME IS 'PEDV~~~STW009 - Previous value of CDV-Candidate Daily Volume is calculated to establish the threshold validation. Stored so there is no need for repeated calculations.';
COMMENT ON TABLE MO_METER_DPIDXREF IS 'Meter DPID X-Ref---A record showing that the usage recorded by a Meter is in some way used to derived the usage at a Discharge Point.';
COMMENT ON COLUMN MO_METER_DPIDXREF.METERDPIDXREF_PK IS '~~~';
COMMENT ON COLUMN MO_METER_DPIDXREF.MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter';
COMMENT ON COLUMN MO_METER_DPIDXREF.MANUFACTURERSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s  serial number of a meter';
COMMENT ON COLUMN MO_METER_DPIDXREF.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler';
COMMENT ON COLUMN MO_METER_DPIDXREF.INITIALMETERREADDATE IS 'Initial Meter Read Date~~~D3042 - Date of Initial meter read, used only to uniquely identify the meter in data correction transactions. STW012 - Part of FK implementing relationship from Meter DPID X-Ref to Meter';
COMMENT ON COLUMN MO_METER_DPIDXREF.SPID IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN MO_METER_DPIDXREF.DPEFFECTFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_METER_DPIDXREF.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_METER_DPIDXREF.EFFECTIVETODATE IS 'Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective to';
COMMENT ON COLUMN MO_METER_DPIDXREF.PERCENTAGEDISCHARGE IS 'MDVol~~~D3024 - For a meter Discharge Point association, the percentage of the volume associated with a meter which is discharged to the Discharge Point';
COMMENT ON COLUMN MO_CUST_ADDRESS.ADDRESSPROPERTY_PK IS '~~~';
COMMENT ON COLUMN MO_CUST_ADDRESS.ADDRESS_PK IS '~~~STW014-Internal key to enable joins - MO_ADDRESS.ADDRESS_PK';
COMMENT ON COLUMN MO_CUST_ADDRESS.CUSTOMERNUMBER_PK IS '~~~STW014-Internal key to enable joins - MO_CUSTOMER.CUSTOMERNUMBER_PK';
COMMENT ON COLUMN MO_PROPERTY_ADDRESS.ADDRESS_PK IS '~~~STW014-Internal key to enable joins - MO_ADDRESS.ADDRESS_PK';
COMMENT ON COLUMN MO_PROPERTY_ADDRESS.STWPROPERTYNUMBER_PK IS '~~~STW014-Internal key to enable joins - MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK';
COMMENT ON COLUMN MO_METER_ADDRESS.METERSERIALNUMBER_PK IS '~~~STW014-Internal key to enable joins - MO_METER.MANUFATURERSERIALNUM_PK';
COMMENT ON COLUMN MO_METER_ADDRESS.ADDRESS_PK IS '~~~STW014-Internal key to enable joins - MO_ADDRESS.ADDRESS_pk';
COMMENT ON COLUMN MO_ADDRESS.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN MO_ADDRESS.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';
COMMENT ON COLUMN MO_ADDRESS.PROPERTYNUMBERPROPERTY IS '~~~STW020 - Property number';
COMMENT ON COLUMN MO_ADDRESS.CUSTOMERNUMBERPROPERTY IS '~~~';
COMMENT ON COLUMN MO_ADDRESS.UPRNREASONCODE IS 'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
COMMENT ON COLUMN MO_ADDRESS.SECONDADDRESABLEOBJECT IS '~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN MO_ADDRESS.PRIMARYADDRESSABLEOBJECT IS '~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN MO_ADDRESS.ADDRESSLINE01 IS 'Address Line 1~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_ADDRESS.ADDRESSLINE02 IS 'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_ADDRESS.ADDRESSLINE03 IS 'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_ADDRESS.ADDRESSLINE04 IS 'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_ADDRESS.ADDRESSLINE05 IS 'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN MO_ADDRESS.POSTCODE IS 'Postcode~~~D5009 - Postcode (without spaces)';
COMMENT ON COLUMN MO_ADDRESS.COUNTRY IS 'Country~~~D5010 - Country, if address is outside the UK';
COMMENT ON COLUMN MO_ADDRESS.LOCATIONFREETEXTDESCRIPTOR IS 'Free Descriptor~~~D5001 - Free text descriptor for address/location  details';

--ALTER_TABLE_DDL
--MO_TARIFF
COMMENT ON TABLE MO_TARIFF IS 'Master tafiff table.' ;
COMMENT ON COLUMN MO_TARIFF.TARIFFCODE_PK IS 'Tariff Code~~~D7001 - A short code specified by the Wholesaler for the Tariff. The code should be meaningful. This code is unique to the Wholesaler for the Service Component. The code can never be changed and must be unique.';
COMMENT ON COLUMN MO_TARIFF.SERVICECOMPONENTREF_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF.TARIFFEFFECTIVEFROMDATE IS 'Tariff Effective From Date~~~D7002 - The earliest date that a Tariff can be applied. This date can never be changed';
COMMENT ON COLUMN MO_TARIFF.TARIFFNAME IS 'Tariff Name~~~D7003 - A friendly name for the Tariff. This name may be updated from time to time';
COMMENT ON COLUMN MO_TARIFF.TARIFFSTATUS IS 'Tariff Status~~~D7004 - A flag indicating whether the Tariff can be applied to Service Components. A Wholesaler may only mark a Tariff as being Active or Legacy, where the Tariff can no longer be newly applied to Service Components but where it is still applied to other Service Components. The Market Operator shall appropriately mark Legacy Tariffs as being Retired Tariffs, once a Legacy Tariff is no longer applied to any Service Components at Tradable SPIDs. Once a Tariff has been marked as a Retired Tariff, its status cannot be changed and it can no longer be applied to Service Components';
COMMENT ON COLUMN MO_TARIFF.TARIFFLEGACYEFFECTIVEFROMDATE IS 'Legacy Tariff Effective From Date~~~D7005 - The date that a Tariff has its Tariff Status set to Legacy. This date can be changed, which allows the Tariff Status of a Tariff to be changed back from Legacy to Active';
COMMENT ON COLUMN MO_TARIFF.APPLICABLESERVICECOMPONENT IS 'Applicable Service Component~~~D7006 - Identifies which Service Component a Tariff can be applied to';
COMMENT ON COLUMN MO_TARIFF.TARIFFAUTHCODE IS 'Tariff Authorisation Code~~~D7007 - The Authorisation Code provided by a Wholesaler when submitting a Tariff, which can be used by the Trading Party to retain an audit trail of authorised Tariff submissions';
COMMENT ON COLUMN MO_TARIFF.VACANCYCHARGINGMETHODWATER IS 'Vacancy Charging Method Water~~~D7052 - The Wholesalers selected method for charging for Water Services where an Eligible Premises is vacant';
COMMENT ON COLUMN MO_TARIFF.VACANCYCHARGINGMETHODSEWERAGE IS 'Vacancy Charging Method Sewerage~~~D7053 - The Wholesalers selected method for charging for Sewerage Services where an Eligible Premises is vacant';
COMMENT ON COLUMN MO_TARIFF.TEMPDISCONCHARGINGMETHODWAT IS 'Temporary Disconnection Charging Method Water~~~D7054 - The Wholesalers selected method for charging for Water Services where an Eligible Premises is temporarily disconnected';
COMMENT ON COLUMN MO_TARIFF.TEMPDISCONCHARGINGMETHODSEW IS 'Temporary Disconnection Charging Method Sewerage~~~D7055 - The Wholesalers selected method for charging for Sewerage Services where an Eligible Premises is temporarily disconnected';

--MO_TARIFF_VERSION
COMMENT ON TABLE MO_TARIFF_VERSION IS 'Master TARIFF Version table';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFFCODE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFFVERSION IS 'Tariff Code~~~D7001 - A short code specified by the Wholesaler for the Tariff. The Tariff Code should be meaningful. This code is unique to the Wholesaler for the Service Component and can never be changed';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFFVEREFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFFSTATUS IS 'Tariff Status~~~A flag indicating whether the Tariff can be applied to Service Components. A Wholesaler may only mark a Tariff as being Active or Legacy, where the Tariff can no longer be newly applied to Service Components but where it is still applied to other Service Components. The Market Operator shall appropriately mark Legacy Tariffs as being Retired Tariffs, once a Legacy Tariff is no longer applied to any Service Components at Tradable SPIDs. Once a Tariff has been marked as a Retired Tariff, its status cannot be changed and it can no longer be applied to Service Components';
COMMENT ON COLUMN MO_TARIFF_VERSION.APPLICABLESERVICECOMPONENT IS 'Applicable Service Component~~~D7006 - Within a Tariff data set, this Data Item identifies which Service Component a Tariff can be applied to. This is similar to Service Component Type (D2043), which identifies a Service Component with respect to a SPID';
COMMENT ON COLUMN MO_TARIFF_VERSION.DEFAULTRETURNTOSEWER IS 'Default Return to Sewer~~~D7051 - The default value for the percentage Volume which is deemed to return to sewer.  This is used where sewerage charges are calculated from metered potable, metered non-potable and private water supplies. Where there is a variation from the default value, this should be adjusted on a case by case basis by the Wholesaler in accordance with CSD 0104 (Maintain SPID Data)';
COMMENT ON COLUMN MO_TARIFF_VERSION.TARIFFCOMPONENTTYPE IS '~~~';
COMMENT ON COLUMN MO_TARIFF_VERSION.SECTION154PAYMENTVALUE IS '~~~D7601 - The value of each individual payment made under Section 154A of the Water Industry Act 1991 Variable name: Sec145AValue Units: £/a';

--MO_TARIFF_TYPE_MPW
COMMENT ON COLUMN MO_TARIFF_TYPE_MPW.MPWSUPPLYPOINTFIXEDCHARGES IS '~~~D7101 - Variable name: MWMFC Units: (mm) - > £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_MPW.MPWPREMIUMTOLFACTOR IS '~~~D7105 - Variable name: PremTolFactorUnits: percentage';
COMMENT ON COLUMN MO_TARIFF_TYPE_MPW.MPWDAILYSTANDBYUSAGEVOLCHARGE IS '~~~D7106 - Variable name: MWDSUVC Units: £/m3';
COMMENT ON COLUMN MO_TARIFF_TYPE_MPW.MPWDAILYPREMIUMUSAGEVOLCHARGE IS '~~~D7107 - Variable name: MWDPUVC Units: £/m3';
COMMENT ON COLUMN MO_TARIFF_TYPE_MPW.MPWMAXIMUMDEMANDTARIFF IS '~~~D7108 - Variable name: MWMDT Units: (£/a per m3/day)';

--MO_MPW_METER_MWMFC
COMMENT ON TABLE MO_MPW_METER_MWMFC IS 'Metered Potable Water, Metered Fixed Charges';
COMMENT ON COLUMN MO_MPW_METER_MWMFC.TARIFF_MWMFC_PK IS '~~~';
COMMENT ON COLUMN MO_MPW_METER_MWMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MPW_METER_MWMFC.LOWERMETERSIZE IS '~~~D7101 - Variable name: MWMFC Units: (mm)  -  Lower Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_MPW_METER_MWMFC.UPPERMETERSIZE IS '~~~D7101 - Variable name: MWMFC Units: (mm)  -  Upper Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_MPW_METER_MWMFC.CHARGE IS '~~~D7101 - Variable name: MWMFC Units: (mm) - > £/a  - Charge - derived form lookup table';
--MO_MPW_BLOCK_MWBT
COMMENT ON TABLE MO_MPW_BLOCK_MWBT IS 'Metered Potable Water, Metered Volumetric Charges';
COMMENT ON COLUMN MO_MPW_BLOCK_MWBT.TARIFF_MWBT_PK IS '~~~';
COMMENT ON COLUMN MO_MPW_BLOCK_MWBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MPW_BLOCK_MWBT.UPPERANNUALVOL IS '~~~D7103 - Variable name: MWBT Units: (m3/a) - Upper Annual Volume - derived from lookup table';
COMMENT ON COLUMN MO_MPW_BLOCK_MWBT.CHARGE IS '~~~D7103 - Variable name: MWBT Units: (m3/a) - Charge - derived from lookup table';

--MO_MPW_STANDBY_MWCAPCHG
COMMENT ON TABLE MO_MPW_STANDBY_MWCAPCHG IS 'Metered Potable Water, Standy Capacity Charges';

--MO_TARIFF_TYPE_MNPW
COMMENT ON TABLE MO_TARIFF_TYPE_MNPW IS 'Metered Non-Potable Water Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.MNPWSUPPLYPOINTFIXEDCHARGE IS '~~~D7102 - Variable name: MWSPFC Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.MNPWPREMIUMTOLFACTOR IS '~~~D7105 - Variable name: PremTolFactor Units: percentage';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.MNPWDAILYSTANDBYUSAGEVOLCHARGE IS '~~~D7106 - Variable name: MWDSUVC Units: £/m3 must be set if MWCapChg is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.MNPWDAILYPREMIUMUSAGEVOLCHARGE IS '~~~D7107 - Variable name: MWDPUVC Units: £/m3 must be set if MWCapChg is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_MNPW.MNPWMAXIMUMDEMANDTARIFF IS '~~~D7108 - Variable name: MWMDT Units: (£/a per m3/day) must be set if MWCapChg is not None';

--MO_MNPW_METER_MWMFC
COMMENT ON TABLE MO_MNPW_METER_MWMFC IS 'Metered Non-Potable Water, Metered Fixed Charges';
COMMENT ON COLUMN MO_MNPW_METER_MWMFC.TARIFF_MWMFC_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_METER_MWMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_METER_MWMFC.LOWERMETERSIZE IS '~~~D7151 - Variable name: MWMFC Units: (mm)  -  Lower Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_MNPW_METER_MWMFC.UPPERMETERSIZE IS '~~~D7151 - Variable name: MWMFC Units: (mm)  -  Upper Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_MNPW_METER_MWMFC.CHARGE IS '~~~D7151 - Variable name: MWMFC Units: (mm) - > £/a  - Charge - derived form lookup table';
--MO_MNPW_BLOCK_MWBT
COMMENT ON TABLE MO_MNPW_BLOCK_MWBT IS 'Metered Non-Potable Water, Metered Volumetric Charges';
COMMENT ON COLUMN MO_MNPW_BLOCK_MWBT.TARIFF_MWBT_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_BLOCK_MWBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_BLOCK_MWBT.UPPERANNUALVOL IS '~~~D7153 - Variable name: MWBT Units: (m3/a) - Upper Annual Volume - derived from lookup table';
COMMENT ON COLUMN MO_MNPW_BLOCK_MWBT.CHARGE IS '~~~D7153 - Variable name: MWBT Units: (m3/a) - Charge - derived from lookup table';
--MO_MNPW_STANDBY_MWCAPCHG
COMMENT ON TABLE MO_MNPW_STANDBY_MWCAPCHG IS 'Metered Non-Potable Water, Standy Capacity Charges';
COMMENT ON COLUMN MO_MNPW_STANDBY_MWCAPCHG.TARIFF_MWCAPCHG_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_STANDBY_MWCAPCHG.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MNPW_STANDBY_MWCAPCHG.RESERVATIONVOLUME IS '~~~D7154 - Variable name: MWCapChgUnits: (m3/day) - Reservation Volume - derived form lookup table';
COMMENT ON COLUMN MO_MNPW_STANDBY_MWCAPCHG.CHARGE IS '~~~D7154 - Variable name: MWCapChgUnits: (m3/day)-> £/a per - Charge - derived from lookup table';
--MO_TARIFF_TYPE_AW
COMMENT ON TABLE MO_TARIFF_TYPE_AW IS 'Assessed Water Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_AW.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_AW.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_AW.AWFIXEDCHARGE IS 'Assessed Water Fixed Charge~~~D7201 - Variable name: AWFixedCharge Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_AW.AWVOLUMETRICCHARGE IS 'Assessed Water Volumetric Charge~~~D7203 - Variable name: AWVCharge Units: £/m3';
--MO_AW_METER_AWMFC
COMMENT ON TABLE MO_AW_METER_AWMFC IS 'Assessed Water Tariff, Metered Fixed Charges';
COMMENT ON COLUMN MO_AW_METER_AWMFC.TARIFF_AWMFC_PK IS '~~~';
COMMENT ON COLUMN MO_AW_METER_AWMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_AW_METER_AWMFC.LOWERMETERSIZE IS '~~~D7202 - Variable name: AWMFC Units: (mm)-Lower Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_AW_METER_AWMFC.UPPERMETERSIZE IS '~~~D7202 - Variable name: AWMFC Units: (mm)-Upper Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_AW_METER_AWMFC.CHARGE IS 'Charge~~~D7202 - Variable name: AWMFC Units: (mm)-Charge - derived from lookup table';
--MO_AW_BAND_CHARGE
COMMENT ON TABLE MO_AW_BAND_CHARGE IS 'Assessed Water Tariff, Band Charge' ;
COMMENT ON COLUMN MO_AW_BAND_CHARGE.TARIFF_BAND_CHARGE_PK IS '~~~';
COMMENT ON COLUMN MO_AW_BAND_CHARGE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_AW_BAND_CHARGE.BAND IS '~~~D7204 - Variable name: AWBandChargeUnits: numerical band - BandID - derived from lookup table';
COMMENT ON COLUMN MO_AW_BAND_CHARGE.CHARGE IS 'Charge~~~D7204 - Variable name: AWBandChargeUnits: numerical band-Charge - derived from lookup table';
--MO_TARIFF_TYPE_UW
COMMENT ON TABLE MO_TARIFF_TYPE_UW IS 'Unmeasured Water Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWFIXEDCHARGE IS 'Unmeasured Water Fixed Charge~~~D7251 - Variable name: UWFixedCharge Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWRVPOUNDAGE IS 'Unmeasured Water RV Poundage~~~D7252 - Variable name: UWRVPoundage Units: £/a per £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWRVTHRESHOLD IS 'Unmeasured Water RV Threshold~~~D7253 - Variable name: UWRVThresh Units: £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWRVMAXCHARGE IS 'Unmeasured Water RV Maximum Charge~~~D7254 - Variable name: UWRVMaxCharge Units: (£/a) optional maximum charge even if UWRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWRVMINCHARGE IS 'Unmeasured Water RV Minimum Charge~~~D7255 - Variable name: UWRVMinCharge Units: (£/a) optional minimum charge even if UWRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEACHARGE IS 'Unmeasured Water Miscellaneous Type A Charge~~~D7256 - Variable name: UWMiscChargeA Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEBCHARGE IS 'Unmeasured Water Miscellaneous Type B Charge~~~D7257 - Variable name: UWMiscChargeB Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPECCHARGE IS 'Unmeasured Water Miscellaneous Type C Charge~~~D7258 - Variable name: UWMiscChargeC Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEDCHARGE IS 'Unmeasured Water Miscellaneous Type D Charge~~~D7259 - Variable name: UWMiscChargeD Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEECHARGE IS 'Unmeasured Water Miscellaneous Type E Charge~~~D7260 - Variable name: UWMiscChargeE Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEFCHARGE IS 'Unmeasured Water Miscellaneous Type F Charge~~~D7261 - Variable name: UWMiscChargeF Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEGCHARGE IS 'Unmeasured Water Miscellaneous Type G Charge~~~D7262 - Variable name: UWMiscChargeG Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_UW.UWMISCTYPEHCHARGE IS 'Unmeasured Water Miscellaneous Type H Charge~~~D7263 - Variable name: UWMiscChargeH Units: £/a';
--MO_UW_METER_UWPFC
COMMENT ON TABLE MO_UW_METER_UWPFC IS 'Unmeasured Water, Pipe Fixed Charges';
COMMENT ON COLUMN MO_UW_METER_UWPFC.TARIFF_UWPFC_PK IS '~~~';
COMMENT ON COLUMN MO_UW_METER_UWPFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_UW_METER_UWPFC.LOWERMETERSIZE IS '~~~D7264 - Variable name: UWPFC Units: (mm)-Lower meter size - derived from lookup table';
COMMENT ON COLUMN MO_UW_METER_UWPFC.UPPERMETERSIZE IS '~~~D7264 - Variable name: UWPFC Units: (mm)-Upper meter size - derived from lookup table';
COMMENT ON COLUMN MO_UW_METER_UWPFC.CHARGE IS '~~~D7264 - Variable name: UWPFC Units: (mm)-Charge - derived from lookup table';
--MO_TARIFF_TYPE_MS
COMMENT ON TABLE MO_TARIFF_TYPE_MS IS 'Metered Sewerage Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_MS.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_MS.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_MS.MSSUPPLYPOINTFIXEDCHARGES IS 'Metered Sewerage Supply Point Fixed Charges~~~D7302 - Variable name: MSSPFC Units: £/a ';
COMMENT ON COLUMN MO_TARIFF_TYPE_MS.MSEFFECTIVEFROMDATE IS '~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
--MO_MS_BLOCK_MSBT
COMMENT ON TABLE MO_MS_BLOCK_MSBT IS 'Metered Sewerage, Volumetric Charges';
COMMENT ON COLUMN MO_MS_BLOCK_MSBT.TARIFF_MWBT_PK IS '~~~';
COMMENT ON COLUMN MO_MS_BLOCK_MSBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MS_BLOCK_MSBT.UPPERANNUALVOL IS '~~~D7303 - Variable name: MSBT Units: (m3/a)-Upper annual volume - derived from lookup table';
COMMENT ON COLUMN MO_MS_BLOCK_MSBT.CHARGE IS '~~~D7303 - Variable name: MSBT Units: (m3/a)-Charge - derived from lookup table';
--MO_MS_METER_MSMFC
COMMENT ON TABLE MO_MS_METER_MSMFC IS 'Metered Sewerage, Fixed Charges';
COMMENT ON COLUMN MO_MS_METER_MSMFC.TARIFF_MSMFC_PK IS '~~~';
COMMENT ON COLUMN MO_MS_METER_MSMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_MS_METER_MSMFC.LOWERMETERSIZE IS '~~~D7301 - Variable name: MSMFC Units: (mm)-Lower meter size - derived from lookup table';
COMMENT ON COLUMN MO_MS_METER_MSMFC.UPPERMETERSIZE IS '~~~D7301 - Variable name: MSMFC Units: (mm)-Upper meter size - derived form lookup table';
COMMENT ON COLUMN MO_MS_METER_MSMFC.CHARGE IS '~~~D7301 - Variable name: MSMFC Units: (mm)-Charge - derived from lookup';
--MO_TARIFF_TYPE_AS
COMMENT ON TABLE MO_TARIFF_TYPE_AS IS 'Assessed Sewerage Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_AS.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_AS.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_AS.ASFIXEDCHARGE IS 'Assessed Sewerage Fixed Charge~~~D7351 - Variable name: ASFixedCharge Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_AS.ASVOLMETCHARGE IS 'Assessed Sewerage Volumetric Charge~~~D7353 - Variable name: ASVCharge Units: £/m3';
COMMENT ON COLUMN MO_TARIFF_TYPE_AS.ASEFFECTIVEDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
--MO_AS_METER_ASMFC
COMMENT ON TABLE MO_AS_METER_ASMFC IS 'Assessed Sewerage, Fixed Charges';
COMMENT ON COLUMN MO_AS_METER_ASMFC.TARIFF_ASMFC_PK IS '~~~';
COMMENT ON COLUMN MO_AS_METER_ASMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_AS_METER_ASMFC.LOWERMETERSIZE IS '~~~D7352 - Variable name: ASMFC Units: (mm)-Lower meter size -  derived from lookup table';
COMMENT ON COLUMN MO_AS_METER_ASMFC.UPPERMETERSIZE IS '~~~D7352 - Variable name: ASMFC Units: (mm)-Upper meter size - derived from lookup table';
COMMENT ON COLUMN MO_AS_METER_ASMFC.CHARGE IS '~~~D7352 - Variable name: ASMFC Units: (mm)-Charge - derived from lookup table';
--MO_AS_BAND_CHARGE
COMMENT ON TABLE MO_AS_BAND_CHARGE IS 'Assessed Sewerage, Band Charges';
COMMENT ON COLUMN MO_AS_BAND_CHARGE.TARIFF_BAND_CHARGE_PK IS '~~~';
COMMENT ON COLUMN MO_AS_BAND_CHARGE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_AS_BAND_CHARGE.BAND IS '~~~D7354 - Variable name: ASBandCharge Units: numerical band -Band ID - derived from lookup table';
COMMENT ON COLUMN MO_AS_BAND_CHARGE.CHARGE IS '~~~D7354 - Variable name: ASBandCharge Units: numerical band - Charge - derived from lookup table';
--MO_TARIFF_TYPE_US
COMMENT ON TABLE MO_TARIFF_TYPE_US IS 'Unmeasured Sewerage Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USFIXEDCHARGE IS 'Unmeasured Sewerage Fixed Charge~~~D7401 - Variable name: USFixedCharge Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USRVPOUNDAGE IS 'Unmeasured Sewerage RV Poundage~~~D7402 - Variable name: USRVPoundage Units: £/a per £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USRVTHRESHOLD IS 'Unmeasured Sewerage RV Threshold~~~D7403 - Variable name: USRVThresh Units: £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USRVMAXIMUMCHARGE IS 'Unmeasured Sewerage RV Maximum Charge~~~D7404 - Variable name: USRVMaxCharge Units: (£/a) Optional maximum charge even if USRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USRVMINIMUMCHARGE IS 'Unmeasured Sewerage RV Minimum Charge~~~D7405 - Variable name: USRVMinCharge Units: (£/a) optional minimum charge even if USRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEACHARGE IS 'Unmeasured Sewerage Miscellaneous Type A Charge~~~D7406 - Variable name: USMiscChargeA Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEBCHARGE IS 'Unmeasured Sewerage Miscellaneous Type B Charge~~~D7407 - Variable name: USMiscChargeB Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPECCHARGE IS 'Unmeasured Sewerage Miscellaneous Type C Charge~~~D7408 - Variable name: USMiscChargeC Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEDCHARGE IS 'Unmeasured Sewerage Miscellaneous Type D Charge~~~D7409 - Variable name: USMiscChargeD Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEECHARGE IS 'Unmeasured Sewerage Miscellaneous Type E Charge~~~D7410 - Variable name: USMiscChargeE Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEFCHARGE IS 'Unmeasured Sewerage Miscellaneous Type F Charge~~~D7411 - Variable name: USMiscChargeF Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEGCHARGE IS 'Unmeasured Sewerage Miscellaneous Type G Charge~~~D7412 - Variable name: USMiscChargeG Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_US.USMISCTYPEHCHARGE IS 'Unmeasured Sewerage Miscellaneous Type H Charge~~~D7413 - Variable name: USMiscChargeH Units: £/a';
--MO_US_METER_USPFC
COMMENT ON TABLE MO_US_METER_USPFC IS 'Unmeasured Sewerage, Pipe Fixed Charges';
COMMENT ON COLUMN MO_US_METER_USPFC.TARIFF_USPFC_PK IS '~~~';
COMMENT ON COLUMN MO_US_METER_USPFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_US_METER_USPFC.LOWERMETERSIZE IS '~~~D7414 - Variable name: USPFC Units: (mm)-Lower meter size - derived from lookup table';
COMMENT ON COLUMN MO_US_METER_USPFC.UPPERMETERSIZE IS '~~~D7414 - Variable name: USPFC Units: (mm)-Upper meter size - derived from lookup table';
COMMENT ON COLUMN MO_US_METER_USPFC.CHARGE IS '~~~D7414 - Variable name: USPFC Units: (mm)-Charge - derived from lookup table';
--MO_TARIFF_TYPE_SW
COMMENT ON TABLE MO_TARIFF_TYPE_SW IS 'Surface Water Drainage Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWCOMBAND IS 'Surface Water Community Band~~~D7453 - Variable name: SWComBand Units: band May not be None if SWAreaBand is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWFIXEDCHARGE IS 'Surface Water Fixed Charge~~~D7454 - Variable name: SWFixedCharge Units: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWRVPOUNDAGE IS 'Surface Water RV Poundage~~~D7455 - Variable name: SWRVPoundage Units: £/a per £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWRVTHRESHOLD IS 'Surface Water RV Threshold~~~D7456 - Variable name: SWRVThresh Units: £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWRVMAXIMUMCHARGE IS 'Surface Water RV Maximum Charge~~~D7457 - Variable name: SWRVMaxCharge Units: (£/a) Optional maximum charge even if SWRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWRVMINIMUMCHARGE IS 'Surface Water RV Minimum Charge~~~D7458 - Variable name: SWRVMinCharge Units: (£/a) optional minimum charge even if SWRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_SW.SWMETERFIXEDCHARGES IS 'Surface Water Meter Fixed Charges~~~D7459 - Variable name: SWMFC Units: (mm)-> £/a';
--MO_SW_AREA_BAND
COMMENT ON TABLE MO_SW_AREA_BAND IS 'Surface Water Drainage, Area Band Charges';
COMMENT ON COLUMN MO_SW_AREA_BAND.TARIFF_AREA_BAND_PK IS '~~~';
COMMENT ON COLUMN MO_SW_AREA_BAND.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_SW_AREA_BAND.LOWERAREA IS 'Area Range Min Value~~~D7451 - Variable name: SWAreaBandUnits: area-Lower area band - derived from lookup table';
COMMENT ON COLUMN MO_SW_AREA_BAND.UPPERAREA IS 'Area Range Max Value~~~D7451 - Variable name: SWAreaBandUnits: area-Upper area band - derived from lookup table';
COMMENT ON COLUMN MO_SW_AREA_BAND.BAND IS '~~~D7451 - Variable name: SWAreaBandUnits: area-Band ID - derived from lookup table';
--MO_SW_BAND_CHARGE
COMMENT ON TABLE MO_SW_BAND_CHARGE IS 'Surface Water Drainage, Band Charges';
COMMENT ON COLUMN MO_SW_BAND_CHARGE.TARIFF_BAND_CHARGE_PK IS '~~~';
COMMENT ON COLUMN MO_SW_BAND_CHARGE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_SW_BAND_CHARGE.BAND IS '~~~D7452 - Variable name: SWBandChargeUnits: band-Band ID - derived from lookup table';
COMMENT ON COLUMN MO_SW_BAND_CHARGE.CHARGE IS '~~~D7452 - Variable name: SWBandChargeUnits: band-Charge - derived from lookup table';
--MO_SW_BLOCK_SWBT
COMMENT ON TABLE MO_SW_BLOCK_SWBT IS 'Surface Water Drainage, Foul Sewerage Volumetric Charges';
COMMENT ON COLUMN MO_SW_BLOCK_SWBT.TARIFF_SWBT_PK IS '~~~';
COMMENT ON COLUMN MO_SW_BLOCK_SWBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_SW_BLOCK_SWBT.UPPERANNUALVOL IS '~~~D7460 - Variable name: SWBT Units: (m3/a)-Charge - derived from lookup table';
COMMENT ON COLUMN MO_SW_BLOCK_SWBT.CHARGE IS '~~~D7460 - Variable name: SWBT Units: (m3/a)-Charge - derived from lookup table';
--MO_SW_METER_SWMFC
COMMENT ON TABLE MO_SW_METER_SWMFC IS 'Surface Water Drainage, Meter Fixed Charges';
COMMENT ON COLUMN MO_SW_METER_SWMFC.TARIFF_SWMFC_PK IS '~~~';
COMMENT ON COLUMN MO_SW_METER_SWMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_SW_METER_SWMFC.LOWERMETERSIZE IS '~~~D7459 - Variable name: SWMFC Units: (mm)-Lower meter size - derived from lookup table';
COMMENT ON COLUMN MO_SW_METER_SWMFC.UPPERMETERSIZE IS '~~~D7459 - Variable name: SWMFC Units: (mm)-Upper meter Size - derived from lookup table';
COMMENT ON COLUMN MO_SW_METER_SWMFC.CHARGE IS '~~~D7459 - Variable name: SWMFC Units: (mm)-Charge - derived from lookup table';
--MO_TARIFF_TYPE_HD
COMMENT ON TABLE MO_TARIFF_TYPE_HD IS 'Highway Drainage Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDCOMBAND IS 'Highway Drainage Community Band~~~D7503 - Variable name: HDComBand Units: band May not be None if HDAreaBand is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDFIXEDCHARGE IS 'Highway Drainage Fixed Charge~~~D7504 - Variable name: HDFixedChargeUnits: £/a';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDRVPOUNDAGE IS 'Highway Drainage RV Poundage~~~D7505 - Variable name: HDRVPoundage Units: £/a per £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDRVTHRESHOLD IS 'Highway Drainage RV Threshold~~~D7506 - Variable name: HDRVThresh Units: £RV';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDRVMAXCHARGE IS 'Highway Drainage RV Maximum Charge~~~D7507 - Variable name: HDRVMaxCharge Units: (£/a) Optional maximum charge even if HDRVPoundage is not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_HD.HDRVMINCHARGE IS 'Highway Drainage RV Minimum Charge~~~D7508 - Variable name: HDRVMinCharge Units: (£/a) Optional maximum charge even if HDRVPoundage is not None';

--MO_HD_AREA_BAND
COMMENT ON TABLE MO_HD_AREA_BAND IS 'Highway Drainage, Area Band Charges';
COMMENT ON COLUMN MO_HD_AREA_BAND.TARIFF_AREA_BAND_PK IS '~~~';
COMMENT ON COLUMN MO_HD_AREA_BAND.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_HD_AREA_BAND.LOWERAREA IS '~~~D7501 - Variable name: HDAreaBandUnits: area - Lower area bands - derived from lookup table';
COMMENT ON COLUMN MO_HD_AREA_BAND.UPPERAREA IS '~~~D7501 - Variable name: HDAreaBandUnits: area - Upper area bands - derived from lookup table';
COMMENT ON COLUMN MO_HD_AREA_BAND.BAND IS '~~~D7501 - Variable name: HDAreaBandUnits: area - Band ID - derived from lookup table';
--MO_HD_BAND_CHARGE
COMMENT ON TABLE MO_HD_BAND_CHARGE IS 'Highway Drainage, Band Charges';
COMMENT ON COLUMN MO_HD_BAND_CHARGE.TARIFF_BAND_CHARGE_PK IS '~~~';
COMMENT ON COLUMN MO_HD_BAND_CHARGE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_HD_BAND_CHARGE.BAND IS '~~~D7502 - Variable name: HDBandChargeUnits: band-Band ID - derived from lookup table';
COMMENT ON COLUMN MO_HD_BAND_CHARGE.CHARGE IS '~~~D7502 - Variable name: HDBandChargeUnits: band-Charge - derived from lookup table';
--MO_HD_BLOCK_HDBT
COMMENT ON TABLE MO_HD_BLOCK_HDBT IS 'Highway Drainage, Foul Sewerage Volumetric Charges';
COMMENT ON COLUMN MO_HD_BLOCK_HDBT.TARIFF_HDBT_PK IS '~~~';
COMMENT ON COLUMN MO_HD_BLOCK_HDBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_HD_BLOCK_HDBT.UPPERANNUALVOL IS '~~~D7510 - Variable name: HDBT Units: (m3/a)-Upper Annual Volume - derived from lookup table';
COMMENT ON COLUMN MO_HD_BLOCK_HDBT.CHARGE IS '~~~D7510 - Variable name: HDBT Units: (m3/a)-Charge - derived from lookup table';
--MO_HD_METER_HDMFC
COMMENT ON TABLE MO_HD_METER_HDMFC IS 'Highway Drainage, Meter Fixed Charges';
COMMENT ON COLUMN MO_HD_METER_HDMFC.TARIFF_HDMFC_PK IS '~~~';
COMMENT ON COLUMN MO_HD_METER_HDMFC.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_HD_METER_HDMFC.LOWERMETERSIZE IS '~~~D7509 - Variable name: HDMFC Units: (mm)-Lower Meter size - derived from lookup table';
COMMENT ON COLUMN MO_HD_METER_HDMFC.UPPERMETERSIZE IS '~~~D7509 - Variable name: HDMFC Units: (mm)-Upper Meter Size - derived from lookup table';
COMMENT ON COLUMN MO_HD_METER_HDMFC.CHARGE IS '~~~D7509 - Variable name: HDMFC Units: (mm)-Charge - derived from lookup table';
--MO_TARIFF_TYPE_TE
COMMENT ON TABLE MO_TARIFF_TYPE_TE IS 'Trade Effluent Tariff Type';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TARIFF_VERSION_PK IS '~~~';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TEFIXEDCHARGE IS 'Trade Effluent Fixed Charge~~~D7571 - Variable name: TEFixedCharge Units: £/a optional fixed charge for each Discharge Point';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPRA IS 'Reception capacity charging component~~~D7552 - Variable name: Ra Units: Units: £/m3 per dayIf one of Ra, Va, Bva, Ma, Ba, Sa, Aa is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPVA IS 'Volumetric/Primary capacity charging component~~~D7553 - Variable name: Va Units: £/m3 per day If one of Ra, Va, Bva, Ma, Ba, Sa, Aa is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPBVA IS 'Additional Volumetric Capacity Charging Component~~~D7554 - Only applies if there is biological treatment Variable name: Bva Units: £/m3 per day';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPMA IS 'Marine Treatment Capacity Charging Component~~~D7555 - Variable name: Ma Units: £/m3 per day';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPBA IS 'Biological capacity charging component~~~D7556 - Variable name: Ba Units: £/kg per day If one of Ra, Va, Bva, Ma, Ba, Sa, Aa,Xa, Ya, Za is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPSA IS 'Sludge capacity charging component~~~D7557 - Variable name: Sa Units: £/kg per day if one of Ra, Va, Bva, Ma, Ba, Sa, Aa, Xa, Ya, Za is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPAA IS 'Ammonia capacity charging component~~~D7558 - Variable name: Aa Units: £/kg per day if one of Ra, Va, Bva, Ma, Ba, Sa, Aa, Xa, Ya, Za is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPVO IS 'Volumetric/Primary charging component~~~D7560 - Variable name: Vo Units: £/m3 if one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPBVO IS 'Additional Volumetric Charging Component~~~D7561 - Only applies if there is biological treatmentVariable name: Bvo Units: £/m3 If one of BTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPMO IS 'Treatment and disposal charging component where effluent is discharged to sea~~~D7562 - Variable name: Mo Units: £/m3 If one of BTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPSO IS 'Sludge Treatment charging component~~~D7564 - Variable name: So Units: £/m3 Iif one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPAO IS 'Ammoniacal Nitrogen charging component~~~D7565 - Variable name: Ao Units: £/m3 If one of BTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPOS IS 'Base value of Chemical Oxygen Demand against which Ot is normalised~~~D7566 - Variable name: Os Units: mg/l if one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPSS IS 'Base value of suspended solids against which St is normalised~~~D7567 - Variable name: Ss Units: mg/l if one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPAS IS 'Base value of Ammoniacal Nitrogen content against which At is normalised~~~D7568 - Base Value against which At is normalised Variable name: As Units: mg/l if one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPAM IS 'Minimum value of Ammoniacal Nitrogen content which is charged~~~D7569 - Variable name: Am Units: mg/lif one of RoBTR, Vo, Bvo, Mo, So, Ao, Os, Ss, As,Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TEMINCHARGE IS 'Trade Effluent Minimum Operational Charge~~~D7570 - Variable name: TEMinCharge Units: (£/a) optional minimum operational charge';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPXA IS 'Trade Effluent Component X Capacity Charging Component~~~D7572 - Variable name: Xa Units: £/kg per day if one of Ra, Va, Bva, Ma, Ba, Sa, Aa is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPYA IS 'Trade Effluent Component Y Capacity Charging Component~~~D7573 - Variable name: Ya Units: £/kg per day if one of Ra, Va, Bva, Ma, Ba, Sa, Aa is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPZA IS 'Trade Effluent Component Z Capacity Charging Component~~~D7574 - Variable name: Za Units: £/kg per day if one of Ra, Va, Bva, Ma, Ba, Sa, Aa is not None must be not None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPXO IS 'Trade Effluent Component X Charging Component~~~D7575 - Variable name: Xo Units: £/m3 if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPYO IS 'Trade Effluent Component Y Charging Component~~~D7576 - Variable name: Yo Units: £/m3 if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPZO IS 'Trade Effluent Component Z Charging Component~~~D7577 - Variable name: Zo Units: £/m3 if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPXS IS 'Trade Effluent Component X Base Value~~~D7578 - Base Value against which Xt is normalised Variable name: Xs Units: mg/l if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPYS IS 'Trade Effluent Component Y Base Value~~~D7579 - Base Value against which Yt is normalised Variable name: Ys Units: mg/l if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPZS IS 'Trade Effluent Component Z Base Value~~~D7580 - Base Value against which Zt is normalised Variable name: Zs Units: mg/l  if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPXM IS 'Minimum value of Trade Effluent Component X which is charged~~~D7581 - Variable name: Xm Units: mg/l if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPYM IS 'Minimum value of Trade Effluent Component Y which is charged~~~D7582 - Variable name: Ym Units: mg/l if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';
COMMENT ON COLUMN MO_TARIFF_TYPE_TE.TECHARGECOMPZM IS 'Minimum value of Trade Effluent Component Z which is charged~~~D7583 - Variable name: Zm Units: mg/l if one of RoBT, Vo, Bvo, Mo, So, Ao, Os, Ss, As, Am, Xo, Yo, Zo, Xs, Ys, Zs, Xm, Ym, Zm is not None then must not be None';

--MO_TE_BAND_CHARGE
COMMENT ON TABLE MO_TE_BAND_CHARGE IS 'Trade Effluent, Band Charges';
COMMENT ON COLUMN MO_TE_BAND_CHARGE.TARIFF_BAND_CHARGE_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BAND_CHARGE.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BAND_CHARGE.BAND IS '~~~D7551 - Variable name: TEBandChargeUnits: band-Band ID - derived from lookup table';
COMMENT ON COLUMN MO_TE_BAND_CHARGE.CHARGE IS 'Charge~~~D7551 - Variable name: TEBandChargeUnits: band-Charge - derived from lookup table';
--MO_TE_BLOCK_ROBT
COMMENT ON TABLE MO_TE_BLOCK_ROBT IS 'Trade Effluent, Availability Charges';
COMMENT ON COLUMN MO_TE_BLOCK_ROBT.TARIFF_ROBT_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BLOCK_ROBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BLOCK_ROBT.UPPERANNUALVOL IS '~~~D7559 - Variable name: RoBT Units: (m3/a) - Upper Annual Volume - derived from lookup table';
COMMENT ON COLUMN MO_TE_BLOCK_ROBT.CHARGE IS 'Charge~~~D7559 - Variable name: RoBT Units: (m3/a) - Charge - derived from lookup table';
--MO_TE_BLOCK_BOBT
COMMENT ON TABLE MO_TE_BLOCK_BOBT IS 'Trade Effluent, Availability Charges';
COMMENT ON COLUMN MO_TE_BLOCK_BOBT.TARIFF_BOBT_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BLOCK_BOBT.TARIFF_TYPE_PK IS '~~~';
COMMENT ON COLUMN MO_TE_BLOCK_BOBT.UPPERANNUALVOL IS '~~~D7563 - Variable name: BoBT Units:  (m3/a) - Upper Annual Volume - derived from lookup table';
COMMENT ON COLUMN MO_TE_BLOCK_BOBT.CHARGE IS 'Charge~~~D7563 - Variable name: BoBT Units:  (m3/a) - Charge - derived from lookup table';
commit;
exit;
