/**************************************************************************************************
Title: Neighbourhood descriptors
Author: Simon Anastasiadis
Modified for NT by JB

Inputs & Dependencies:
- [IDI_Clean].[data].[personal_detail]
- [IDI_Clean].[data].[snz_res_pop]
- [IDI_Clean].[data].[address_notification]
- [IDI_Metadata].[clean_read_CLASSIFICATIONS].[meshblock_concordance_2019]
- [IDI_Metadata].[clean_read_CLASSIFICATIONS].[meshblock_current_higher_geography]
- [IDI_Metadata].[clean_read_CLASSIFICATIONS].[DepIndex2013]
Outputs:
- [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)addr_desc]

Description:
Summary description of a person's neighbourhood including: region,
deprivation, urban/rural

Intended purpose:
Identifying the region, urban/rural-ness, and other characteristics of where a person lives
at a specific point in time.

Notes:
1) Address information in the IDI is not of sufficient quality to determine who shares an
   address. We would also be cautious about claiming that a person lives at a specific
   address on a specific date. However, we are confident using address information for the
   purpose of "this location has the characteristics of the place this person lives", and
   "this person has the characteristics of the people who live in this location".
2) Despite the limitations of address, it is the best source for determining whether a person
   lives in a household with dependent children. Hence we use it for this purpose. However
   we note that this is a low quality measure.
3) The year of the meshblock codes used for the address notification could not be found in
   data documentation. A quality of range of different years/joins were tried the final
   choice represents the best join available at time of creation.
   Another cause for this join being imperfect is not every meshblock contains residential
   addresses (e.g. some CBD areas may contain hotels but not residential addresses, and
   some meshblocks are uninhabited - such as mountains or ocean areas).
   Re-assessment of which meshblock code to use for joining to address_notifications
   is recommended each refresh.
4) For simplicity this table considers address at a specific date.

Parameters & Present values:
  Current refresh = $(IDIREF)
  Prefix = $(TBLPREF)
  Project schema = $(PROJSCH)

   
Issues:

History (reverse order):
2020-03-03 SA v1
**************************************************************************************************/
--SQLCMD only (Activate by clicking Query->SQLCMD Mode)
/* Already in master.sql; Uncomment if running individually
:setvar TBLPREF "swangt_"
:setvar IDIREF "IDI_Clean_20200120"
:setvar PROJSCH "DL-MAA2020-35"
GO
*/
USE IDI_UserCode

/* Clear existing view */
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)evtvw_addr_desc]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_addr_desc];
GO

/* Create staging */
CREATE VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_addr_desc]
AS
SELECT [pop].[snz_uid]
	,a.[ant_notification_date]
	,a.[ant_replacement_date]
	,a.[snz_idi_address_register_uid]
	,a.[ant_region_code]
	,b.[IUR2018_V1_00] -- urban/rural classification
	,b.[IUR2018_V1_00_NAME]
	,CAST(b.[SA22018_V1_00] AS INT) AS [SA22018_V1_00] -- Statistical Area 2 (neighbourhood)
	,b.[SA22018_V1_00_NAME]
	,c.[DepIndex2013]
	,c.[DepScore2013]
FROM [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] AS [pop]
LEFT JOIN [$(IDIREF)].[data].[address_notification] AS a ON [pop].[snz_uid] = [a].[snz_uid]
INNER JOIN [IDI_Metadata].[clean_read_CLASSIFICATIONS].[meshblock_concordance_2019] AS conc ON conc.[MB2019_code] = a.[ant_meshblock_code]
LEFT JOIN [IDI_Metadata].[clean_read_CLASSIFICATIONS].[meshblock_current_higher_geography] AS b ON conc.[MB2018_code] = b.[MB2018_V1_00]
LEFT JOIN [IDI_Metadata].[clean_read_CLASSIFICATIONS].[DepIndex2013] AS c ON conc.[MB2013_code] = c.[Meshblock2013]
WHERE a.[ant_meshblock_code] IS NOT NULL --do we have to filter dates?
GO

/* 
Variable Ref 51,52 (process)
Author: Joel Bancolita
EVENT: destinations (value=regc) 'from' age1 (e.g. 18) 
*/
/* Clear existing view */
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)evtvw_regc_movement]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_regc_movement];
GO

/* Create staging */
CREATE VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_regc_movement]
AS
SELECT [addr].snz_uid
	,[addr].ant_notification_date AS [start_date]
	,format(Iif([addr].ant_replacement_date > getdate(), getdate(), [addr].ant_replacement_date), 'yyyy-MM-dd') AS [end_date]
	,Iif(dob_atage1 BETWEEN [ant_notification_date]
			AND [ant_replacement_date]
				,'address:from'
				,'address:to') AS [description]
	,[addr].ant_region_code AS [value] --region
	,'address notification' AS [source]
FROM [IDI_usercode].[$(PROJSCH)].[$(TBLPREF)evtvw_addr_desc] [addr]
INNER JOIN [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] [pop] ON [addr].[snz_uid] = [pop].[snz_uid]
WHERE (
		dob_atage1 BETWEEN [ant_notification_date]
			AND [ant_replacement_date]
		)
	OR dob_atage1 <= [ant_notification_date]
GO


