/**************************************************************************************************************
Title: Nga Tapuae population definition
Author: Joel Bancolita
		Social Wellbeing Agency

Reviewer: Simon Anastasiadis
Intended use: 
Define the population and create table of snz_uid, reference year, address source, sex, birthdate (including for age window of interest, e.g. 15-18)
Notes:
This is predominantly population 2 (15-23 year-olds) -specific

History (reverse order): 
20200804: JB tweaked population derivation: continued tweak
20200803: JB tweaked population derivation: get people who turned $(AGE) during 2013-2018 as long as they are in res_pop instead of being in res_pop during those years-->increased our popn 
20200723: JB add parents--births, civil unions, marriages
20200715: JB tidying
20200714: JB deployment onto project schema
20200603: JB generalise to some extent; add further indices
20200602: JB entity counts
20200528: JB refine rules (rules 1 and 2)
20200527: JB two population defs (18-05-2020 sprint)
202005: JB initialise


Depends:	
- [$(IDIREF)].[data].[personal_detail]
- [$(IDIREF)].[data].[address_notification]
- [$(IDIREF)].[data].[snz_res_pop]
- [$(IDIREF)].[moe_clean].[student_enrol]

**************************************************************************************************************/
--PARAMETERS##################################################################################################
--SQLCMD only (Activate by clicking Query->SQLCMD Mode)
/* Already in master.sql; Uncomment if running individually
:setvar TBLPREF "swangt_"
:setvar IDIREF "IDI_Clean_20200120"
:setvar PROJSCH "DL-MAA2020-35"
GO

--reference year and birth year
--age at reference year/s
:setvar AGE 30 
--age window 0
:setvar AGE0 15 
--age window 1
:setvar AGE1 18 
--age post (initial value  or window)
:setvar AGEPIV 15
--Takiwa regions
:setvar TAKREGC 12,13,14,15
GO
*/

--##############################################################################################################
/*embedded in user code*/
USE IDI_UserCode
GO

--personal details
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)perdet]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)perdet];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)perdet]
AS
SELECT [per].[snz_uid]
	,DATEFROMPARTS([per].snz_birth_year_nbr, [per].snz_birth_month_nbr, 28) AS [dob]
	,/* 28 is set so it's nearer to 30 (ref_date) */
	[per].[snz_sex_code] AS [sex]
	,[per].[snz_ethnicity_grp2_nbr] AS [maori]
FROM [$(IDIREF)].[data].[personal_detail] [per]
INNER JOIN (
	SELECT DISTINCT [snz_uid]
	FROM [$(IDIREF)].[data].[snz_res_pop]
	) [res] /* moved this here from the next query */
	ON [per].[snz_uid] = [res].[snz_uid]
WHERE snz_birth_month_nbr IS NOT NULL
	AND snz_birth_year_nbr IS NOT NULL;
GO

--turned @yr on each 2013,...,2018
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)pop_snz_uid]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)pop_snz_uid];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)pop_snz_uid]
AS
WITH n (n)
AS (
	SELECT 1
	
	UNION ALL
	
	SELECT n + 1
	FROM n
	WHERE n < 6
	)
	,pd
AS (
	SELECT *
	FROM [$(PROJSCH)].[$(TBLPREF)perdet]
	)
SELECT pd.snz_uid
	,n.n + 2012 AS refyr
	,pd.dob
	,datediff(year, convert(VARCHAR, [pd].[dob], 23), convert(VARCHAR, datefromparts(n.n + 2012, 6, 30), 23)) AS age_atref
	,pd.sex
	,pd.maori
FROM n
	,pd
GO

--rule1:people attending school in the takiwa
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)popyr_enrolhs];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)popyr_enrolhs]
AS
SELECT DISTINCT [pop].[refyr]
	,[pop].snz_uid
	,[pop].dob
	,[moe].moe_esi_start_date
	,[moe].moe_esi_end_date
	,[moe].[moe_esi_provider_code]
	,[moe].[moe_esi_entry_year_lvl_nbr]
	,datediff(year, convert(VARCHAR, [pop].[dob], 23), convert(VARCHAR, [moe].moe_esi_start_date, 23)) AS age_atstart
	,datediff(year, convert(VARCHAR, [pop].[dob], 23), convert(VARCHAR, [moe].moe_esi_end_date, 23)) AS age_atend
FROM [IDI_UserCode].[$(PROJSCH)].[$(TBLPREF)pop_snz_uid] [pop]
INNER JOIN [$(IDIREF)].[moe_clean].[student_enrol] [moe] ON [pop].[snz_uid] = [moe].[snz_uid]
WHERE [pop].[age_atref] = $( AGE )
	AND [pop].[maori] = 1
	--and [moe].[moe_esi_entry_year_lvl_nbr] between 9 and 13 --don't put this for now
GO

--table 1: filter people who are attending during 15-18
IF OBJECT_ID('[IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs_tak]', 'U') IS NOT NULL
	DROP TABLE [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs_tak];
		--go

SELECT [pop].*
INTO [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs_tak]
FROM [IDI_UserCode].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs] [pop]
INNER JOIN [IDI_Metadata].[clean_read_CLASSIFICATIONS].[moe_school_profile] [prv] ON [pop].[moe_esi_provider_code] = [prv].SchoolNumber
WHERE (
		(age_atstart >= $( AGE0)
		AND age_atstart <= $( AGE1
		) )
	OR (age_atend >= $( AGE0)
	AND age_atend <= $( AGE1 ) ) ) --attending during 15-18
	AND [prv].SchoolRegion2 IN ($( TAKREGC) ) -- in the takiwa; think of adding year level constraint: 'and [moe_esi_entry_year_lvl_nbr] between 9 and 13'

--create index on snz_uid
CREATE INDEX individx ON [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs_tak] (snz_uid);

--rule2:address notification is in the takiwa
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)popyr_addr]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)popyr_addr]
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)popyr_addr]
AS
SELECT [pop].*
	,[addr].[ant_region_code]
	,[addr].[ant_notification_date]
	,[addr].[ant_replacement_date]
	,iif([pop].[date_atage0] > [addr].[ant_notification_date], [pop].[date_atage0], [addr].[ant_notification_date]) AS [addr_d0]
	,--max(date@15, addrdatenotify)
	iif([pop].[date_atage1] > [addr].[ant_replacement_date], [addr].[ant_replacement_date], dateadd(day, - 1, [pop].[date_atage1])) AS [addr_d1] --min(date@19, addrdaterepl)-1day (before 19th)
FROM (
	SELECT [pop].*
		,dateadd(year, $( AGE0)
		,[pop].dob
	) AS [date_atage0]
	,dateadd(year, $( AGE1) + 1
	,[pop].dob ) AS [date_atage1] --post $(AGE1)
FROM [$(PROJSCH)].[$(TBLPREF)pop_snz_uid] [pop] ) [pop]
INNER JOIN [$(IDIREF)].[data].[address_notification] [addr] ON [pop].[snz_uid] = [addr].[snz_uid]
WHERE age_atref = $( AGE )
	AND maori = 1;
GO

--table in the takiwa
IF OBJECT_ID('[IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak]', 'U') IS NOT NULL
	DROP TABLE [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak];
		--go

SELECT DISTINCT [snz_uid]
	,[refyr]
	,[dob]
	,[age_atref]
	,[sex]
	,[maori]
	,[date_atage0]
	,[date_atage1]
	,[ant_region_code]
INTO [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak]
FROM [IDI_UserCode].[$(PROJSCH)].[$(TBLPREF)popyr_addr]
WHERE [addr_d1] >= [addr_d0]
	AND [addr_d1] < [date_atage1]
	AND [addr_d0] >= [date_atage0]
	AND [ant_region_code] IN ($( TAKREGC) );

--rule1 U rule2
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)popyr_tak_u]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)popyr_tak_u]
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)popyr_tak_u]
AS
SELECT *
FROM (
	SELECT DISTINCT refyr
		,snz_uid
		,1 AS addr_src
	FROM [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_enrolhs_tak] --moe
	
	UNION ALL
	
	SELECT DISTINCT refyr
		,snz_uid
		,2 AS addr_src
	FROM [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak]
	) tak --ant
GO

--normalise: just get one primary source
IF OBJECT_ID('[IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm]', 'U') IS NOT NULL
	DROP TABLE [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm]
GO

SELECT [addr].refyr
	,[addr].snz_uid
	,[addr].addr_src
	,[per].[snz_sex_code] AS sex
	,[per].[snz_birth_date_proxy] AS dob
	,dateadd(year, $( AGE0)
	,[snz_birth_date_proxy] ) AS dob_atage0
	,dateadd(year, $( AGE1)
	,[snz_birth_date_proxy] ) AS dob_atage1
INTO [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm]
FROM (
	SELECT refyr
		,snz_uid
		,min(addr_src) AS addr_src
	FROM [$(PROJSCH)].[$(TBLPREF)popyr_tak_u]
	GROUP BY refyr
		,snz_uid
	) addr
INNER JOIN [$(IDIREF)].[data].[personal_detail] [per] ON [per].[snz_uid] = [addr].[snz_uid]

--create index
CREATE INDEX individx ON [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] (snz_uid);

--######################################################################################################################################
/*
Variable Ref 63, 68, 69
Parents staging
Note: 
- no intermingling, i.e. parent of partner not assigned as parent of the person
- marriage and civil union parents not relevant as no one has marriage or civil union event before the window, i.e. before 15 years old

*/
/* Clear existing view */
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)parents_staging]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)parents_staging];
GO

/*recency decreasing */
CREATE VIEW [$(PROJSCH)].[$(TBLPREF)parents_staging]
AS
SELECT *
FROM (
	SELECT partnr1_snz_uid AS snz_uid
		,dia_civ_civil_union_date AS event_date
		,p1parent1_snz_uid AS parent_id
		,'dia civil_unions' AS source
	FROM [$(IDIREF)].[dia_clean].[civil_unions]
	
	UNION
	
	SELECT partnr1_snz_uid AS snz_uid
		,dia_civ_civil_union_date AS event_date
		,p1parent2_snz_uid AS parent_id
		,'dia civil_unions' AS source
	FROM [$(IDIREF)].[dia_clean].[civil_unions]
	
	UNION
	
	SELECT partnr2_snz_uid AS snz_uid
		,dia_civ_civil_union_date AS event_date
		,p2parent1_snz_uid AS parent_id
		,'dia civil_unions' AS source
	FROM [$(IDIREF)].[dia_clean].[civil_unions]
	
	UNION
	
	SELECT partnr2_snz_uid AS snz_uid
		,dia_civ_civil_union_date AS event_date
		,p2parent2_snz_uid AS parent_id
		,'dia civil_unions' AS source
	FROM [$(IDIREF)].[dia_clean].[civil_unions]
	
	UNION
	
	SELECT partnr1_snz_uid AS snz_uid
		,dia_mar_marriage_date AS event_date
		,p1parent1_snz_uid AS parent_id
		,'dia marriages' AS source
	FROM [$(IDIREF)].[dia_clean].[marriages]
	
	UNION
	
	SELECT partnr1_snz_uid AS snz_uid
		,dia_mar_marriage_date AS event_date
		,p1parent2_snz_uid AS parent_id
		,'dia marriages' AS source
	FROM [$(IDIREF)].[dia_clean].[marriages]
	
	UNION
	
	SELECT partnr2_snz_uid AS snz_uid
		,dia_mar_marriage_date AS event_date
		,p2parent1_snz_uid AS parent_id
		,'dia marriages' AS source
	FROM [$(IDIREF)].[dia_clean].[marriages]
	
	UNION
	
	SELECT partnr2_snz_uid AS snz_uid
		,dia_mar_marriage_date AS event_date
		,p2parent2_snz_uid AS parent_id
		,'dia marriages' AS source
	FROM [$(IDIREF)].[dia_clean].[marriages]
	
	UNION
	
	SELECT [snz_uid]
		,DATEFROMPARTS(dia_bir_birth_year_nbr, dia_bir_birth_month_nbr, 28) AS event_date
		,[parent1_snz_uid] AS parent_id
		,'dia births' AS source
	FROM [$(IDIREF)].[dia_clean].[births]
	
	UNION
	
	SELECT [snz_uid]
		,DATEFROMPARTS(dia_bir_birth_year_nbr, dia_bir_birth_month_nbr, 28) AS event_date
		,[parent2_snz_uid] AS parent_id
		,'dia births' AS source
	FROM [$(IDIREF)].[dia_clean].[births]
	) a
WHERE parent_id IS NOT NULL
GO

/*Just a union of all parent ids and child ids */
/* Clear existing view */
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)child_parent_union]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)child_parent_union];
GO

/*recency decreasing */
CREATE VIEW [$(PROJSCH)].[$(TBLPREF)child_parent_union]
AS
SELECT [parent_id] AS [snz_uid]
	,'id as parent' AS [description]
FROM [IDI_UserCode].[$(PROJSCH)].[$(TBLPREF)parents_staging] [par]
INNER JOIN [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] [pop] ON [par].[snz_uid] = [pop].[snz_uid]

UNION

SELECT [snz_uid]
	,'id as person' AS [Description]
FROM [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] [pop]
GO


