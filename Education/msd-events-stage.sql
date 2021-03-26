/**************************************************************************************************************
Nga Tapuwae MSD events measures

**************************************************************************************************************/

--PARAMETERS##################################################################################################
--SQLCMD only
--SQLCMD only (Activate by clicking Query->SQLCMD Mode)
/* Already in master.sql; Uncomment if running individually
:setvar TBLPREF "swangt_"
:setvar IDIREF "IDI_Clean_20200120"
:setvar PROJSCH "DL-MAA2020-35"
:setvar POP1 "[IDI_Sandpit].[DL-MAA2020-35].[swangt30_popyr_tak_unorm]" 
GO
*/

--##############################################################################################################

/*embedded in user code*/
USE IDI_UserCode
GO


/**************************************************************************************************************
Title: T2 Benefit receipt by type
Author: Michael Hackney and Simon Anastasiadis, et. al. (HaBiSA project), 
Reviewer: Simon Anastasiadis, AK
Intended use: Identify periods of Tier 2 benefit receipt,
sum value of tier 2 benefit received, to identify the types of T2 benefits.

Notes:
Periods of Tier 2 benefit receipt with daily amount received.
The IDI metadata database contains multiple tables that translate benefit type codes
into benefit names/descriptions. The differences between these tables are not well
explained.
Not every code appears in every table, hence for some applications we need to combine
multiple metadata tables.

History (reverse order): 
2019-04-09 QA (AK)
- archived manual list, replaced with join to metadata
- value change to total for period
2019-04-23 Changes applied (AK)
- Code Index for codes 604, 605, 667 not available
- Joining two tables, table meta data not available
2019-04-26 notes added above (SA)
2018-12-06 reveiwed (SA)
2018-12-04 initiated


Depends:	
- popdefn-stage.sql
- [IDI_Clean_20200120].[msd_clean].[msd_second_tier_expenditure]
- [IDI_Sandpit].[DL-MAA2016-15].[tmp_main_benefit_final], created by the codes main_benefits_by_type_and_partner_status.sql
- [IDI_Metadata].[clean_read_CLASSIFICATIONS].[msd_benefit_type_code]
- [IDI_Metadata].[clean_read_CLASSIFICATIONS].[msd_benefit_type_code_4] 
- [IDI_Clean_20200120].[ir_clean].[ird_ems]
- []

**************************************************************************************************************/

IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T2]','V') IS NOT NULL
DROP VIEW [$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T2];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T2] AS

SELECT [t2].snz_uid
	,msd_ste_start_date as [start_date]
	,[msd_ste_end_date] as end_date
	,'msd t2 benefits' AS [description]
--	,[classification] AS [description]
--	,[msd_ste_daily_gross_amt] * (1 + DATEDIFF(DAY, msd_ste_start_date, [msd_ste_end_date])) AS value
	,1 AS [value]
	,'msd t2 benefits' AS [source]
FROM [$(IDIREF)].[msd_clean].[msd_second_tier_expenditure] t2
	inner join [IDI_Sandpit].[$(PROJSCH)].[swangt30_popyr_tak_unorm] [pop]
	on [pop].[snz_uid] = [t2].[snz_uid]
INNER JOIN (
	-- Code classifications
	SELECT [Code], [classification]
	FROM [IDI_Metadata].[clean_read_CLASSIFICATIONS].[msd_benefit_type_code]
	UNION ALL
	SELECT [Code], [classification]
	FROM [IDI_Metadata].[clean_read_CLASSIFICATIONS].[msd_benefit_type_code_4] -- add three codes that do not appear in the first metadata table
	WHERE Code IN (604, 605, 667)
) codes
ON t2.[msd_ste_supp_serv_code] = codes.Code
WHERE [msd_ste_supp_serv_code] IS NOT NULL;
GO


/*
Title: T1 Benefit receipt by type
Author: Simon Anastasiadis, (HaBiSA project) 
DATE: 2018-12-06
Intended use: Identify periods of Tier 1 benefit receipt.

Notes:
Periods of tier 1 benefit receipt (main benefits). Generated by Marc de Boer's
SAS macros as incorporated into the SIAL. Not suited for calculating benefit amount
received but suited for duration of benefit.

History (reverse order): 
2019-04-23 Reviewed (Akhilesh Chokkanathapuram Vasudevan)
2018-12-06 Initiated (SA)
*/


IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T1]','V') IS NOT NULL
DROP VIEW [$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T1];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)BENEFIT_RECEIPT_T1] AS

SELECT [msd].snz_uid
	,[msd].[start_date]
	,[msd].[end_date]
	, 'msd t1 benefits' AS [description]
	,1 AS [value]
	,'msd T1 SIAL' AS [source]
FROM [IDI_Sandpit].[$(PROJSCH)].SIAL_MSD_T1_events [msd]
	inner join [IDI_Sandpit].[$(PROJSCH)].[swangt30_popyr_tak_unorm] [pop]
	on [pop].[snz_uid] = [msd].[snz_uid];
GO


/* 
Title: Student loan debt
Author: Marianna Pekar, Simon Anastasiadis (Smoothing based on Simon Anastasiadis, Debt to govt project) 
Reviewed: awaiting review
Intended use: Identify periods of having a student loan debt to the government.

Notes:
The smoothing part of the codes are based on Simon Anastasiadis' work prepared for the Understand debt to governemnt project
The [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)student_loan_debt] is consequently modified to create a dataset indicating the length of
time while an individual has student loan debt to the governemnt over the threshold of implicit account closure (NZD 20).

History (reverse order): 
2020-08-31 Initiated (MP)

 */

IF OBJECT_ID('[IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)student_loan_debt]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)student_loan_debt];
GO

WITH
/* resolve non-unique values */
distinct_balances AS (
	SELECT [snz_uid]
	      ,[ir_bal_loan_bal_month_date]
		  ,SUM([ir_bal_loan_bal_amt]) AS [ir_bal_loan_bal_amt]
	FROM [$(IDIREF)].[sla_clean].[ird_eom_tot_loan_bal]
	GROUP BY [snz_uid], [ir_bal_loan_bal_month_date]
),
/* smooth values */
lag_lead_balances AS (
	SELECT [snz_uid]
	      ,[ir_bal_loan_bal_month_date]
		  ,[ir_bal_loan_bal_amt]
		  ,LAG([ir_bal_loan_bal_amt], 1) OVER(PARTITION BY snz_uid ORDER BY [ir_bal_loan_bal_month_date]) AS lag_balance
		  ,LEAD([ir_bal_loan_bal_amt], 1) OVER(PARTITION BY snz_uid ORDER BY [ir_bal_loan_bal_month_date]) AS lead_balance
	FROM distinct_balances
),
smoothed_balances AS (
	SELECT [snz_uid]
		,[ir_bal_loan_bal_month_date]
		,CONCAT(YEAR([ir_bal_loan_bal_month_date]),'Q',DATEPART(QUARTER, [ir_bal_loan_bal_month_date])) AS [quarter]
		,[ir_bal_loan_bal_amt]
		,lag_balance
		,lead_balance
		/* smoothing */
		,IIF( ABS([ir_bal_loan_bal_amt] - lag_balance) > 45000 AND ABS([ir_bal_loan_bal_amt] - lead_balance) > 45000,
			 0.5 * (lag_balance + lead_balance), [ir_bal_loan_bal_amt]) AS [smoothed_balance]
	FROM lag_lead_balances
)
SELECT [snz_uid]
	,[ir_bal_loan_bal_month_date]
	,[quarter]
	,[smoothed_balance]
INTO [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)student_loan_debt]
FROM (
	SELECT [snz_uid]
		,[ir_bal_loan_bal_month_date]
		,[quarter]
		,[smoothed_balance]
		,LEAD([quarter], 1) OVER (PARTITION BY snz_uid, [quarter] ORDER BY [ir_bal_loan_bal_month_date]) AS next_month
	FROM smoothed_balances

) k
WHERE [smoothed_balance] > 20 -- threshold for implicit account closure
/* keep the closest value to the end of the quarter where it exists */
AND next_month IS NULL;
GO

/* create a dataset for representative timeline visualisation with the following columns:
snz_uid, start_date, end_date, description, value, source
*/




IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)student_loan_timeline]','V') IS NOT NULL
DROP VIEW [$(PROJSCH)].[$(TBLPREF)student_loan_timeline];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)student_loan_timeline] AS
SELECT snz_uid
	,[start_date]
	,[end_date]
	,[description]
	,[value]
	,[source]
	FROM(
	SELECT 	snz_uid
		,min([start_date]) OVER(PARTITION BY snz_uid, total_debt ORDER BY snz_uid, total_debt) as [start_date]
		,max([end_date]) OVER(PARTITION BY snz_uid, total_debt ORDER BY snz_uid, total_debt) as [end_date]
		,'total student loan debt' as description
		,[total_debt] as value
		,'msd SLA' as source 
		FROM (

		SELECT distinct snz_uid
			,max([smoothed_balance]) OVER (PARTITION BY snz_uid ORDER BY snz_uid) as [total_debt]
			,IIF(last_month_bal is NULL, [ir_bal_loan_bal_month_date],NULL) as [start_date]
			,IIF(next_month_bal is NULL, [ir_bal_loan_bal_month_date],NULL) as [end_date]
			FROM(
			SELECT sld.snz_uid as snz_uid
				,[ir_bal_loan_bal_month_date]
				,[smoothed_balance]
				,LAG([smoothed_balance]) OVER (PARTITION BY sld.snz_uid ORDER BY [ir_bal_loan_bal_month_date]) as last_month_bal
				,LEAD([smoothed_balance]) OVER (PARTITION BY sld.snz_uid ORDER BY [ir_bal_loan_bal_month_date]) as next_month_bal
			FROM [IDI_Sandpit].[$(PROJSCH)].[swangt30_student_loan_debt] as sld
			INNER JOIN  [IDI_Sandpit].[$(PROJSCH)].[swangt30_popyr_tak_unorm] AS pop ON pop.snz_uid = sld.snz_uid
			GROUP BY sld.snz_uid,  [ir_bal_loan_bal_month_date], [smoothed_balance] 
			) a
			WHERE IIF(last_month_bal is NULL, [ir_bal_loan_bal_month_date],NULL) is not NULL or IIF(next_month_bal is NULL, [ir_bal_loan_bal_month_date],NULL) is not NULL
			) b
			) c
			GROUP BY snz_uid, [description], [start_date], [end_date], [value], source;
GO
