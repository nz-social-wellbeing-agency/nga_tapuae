/**************************************************************************************************
Title: Tax year income summary
Author: Simon Anastasiadis, Modified for Nga Tapuae by Joel Bancolita
Reviewer:
Intended use: Calculating annual income FROMdifferent sources and in grand total.
Summary of income for each tax year.
 
Notes:
1) Following a conversatiON with a staff member FROMIRD we were advised to use
   - IR3 data where possible.
   - PTS data where IR3 is not available
   - EMS date where IR3 and PTS are not available.
2) A comparisON of total incomes FROMthese three sources showed excellent consistency
   between [ir_ir3_gross_earnings_407_amt], [ir_pts_tot_gross_earnings_amt], [ir_ems_gross_earnings_amt]
   with more than 90% of our sample of records having identical values across all three.
3) However, rather than combine IR3, PTS and EMS ourselves we use the existing [income_tax_yr_summary]
   table AS it addresses the same concern and is already a standard table.
4) Unlike EMS where W&S and WHP are reported directly, the tax year summary table re-assigns some
   W&S and WHP to the S0*, P0*, C0* categories. Hence sum(WHP) FROMtax year summary will not be
   consistent with sum(WHP) FROMIRD-EMS. You can see in the descriptions that S/C/P01 have PAYE
   and hence will (probably) appear in IRD-EMS AS W&S, while S/C/P02 have WHT deducted and hence
   will (probably) appear in IRD-EMS AS WHP.

 Inputs & Dependencies:
- [IDI_Clean].[data].[income_tax_yr_summary]
Outputs:
- [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)income_tax_year]

Parameters & Present values:
  Current refresh = $(IDIREF)
  Prefix = $(TBLPREF)
  Project schema = [$(PROJSCH)]
 
Issues:
- IR3 records in the IDI do not capture all income reported to IRD via IR3 records. AS per the data
  dictionary only "active items that have non-zero partnership, self-employment, or shareholder salary
  income" are included. So people with IR3 income that is of a different type (e.g. rental income)
  may not appear in the data.
 
History (reverse order):
2020-07-02 JB modifications for NT
2020-03-02 SA v1
**************************************************************************************************/
--SQLCMD only (Activate by clicking Query->SQLCMD Mode)
/* Already in master.sql; Uncomment if running individually
:setvar TBLPREF "swangt_"
:setvar IDIREF "IDI_Clean_20200120"
:setvar PROJSCH "DL-MAA2020-35"
GO
*/
/* Establish database for writing views */
USE IDI_UserCode
GO

/* Wages and salaries by tax year */
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)income_tax_year]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)income_tax_year];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)income_tax_year]
AS
SELECT [snz_uid]
	,[inc_tax_yr_sum_year_nbr]
	,DATEFROMPARTS([inc_tax_yr_sum_year_nbr], 3, 31) AS [event_date]
	,DATEFROMPARTS([inc_tax_yr_sum_year_nbr], 1, 1) AS [start_date]
	,DATEFROMPARTS([inc_tax_yr_sum_year_nbr], 12, 31) AS [end_date]
	,[inc_tax_yr_sum_WAS_tot_amt] /* wages & salaries */
	,[inc_tax_yr_sum_WHP_tot_amt] /* withholding payments (schedular payments with withholding taxes) */
	,[inc_tax_yr_sum_BEN_tot_amt] /* benefits */
	,[inc_tax_yr_sum_ACC_tot_amt] /* ACC claimants compensatiON */
	,[inc_tax_yr_sum_PEN_tot_amt] /* pensions (superannuation) */
	,[inc_tax_yr_sum_PPL_tot_amt] /* Paid parental leave */
	,[inc_tax_yr_sum_STU_tot_amt] /* Student allowance */
	,[inc_tax_yr_sum_C00_tot_amt] /* Company director/shareholder income FROMIR4S */
	,[inc_tax_yr_sum_C01_tot_amt] /* Comapny director/shareholder receiving PAYE deducted income */
	,[inc_tax_yr_sum_C02_tot_amt] /* Company director/shareholder receiving WHT deducted income */
	,[inc_tax_yr_sum_P00_tot_amt] /* Partnership income FROMIR20 */
	,[inc_tax_yr_sum_P01_tot_amt] /* Partner receiving PAYE deducted income */
	,[inc_tax_yr_sum_P02_tot_amt] /* Partner receiving withholding tax deducted income */
	,[inc_tax_yr_sum_S00_tot_amt] /* Sole trader income FROMIR3 */
	,[inc_tax_yr_sum_S01_tot_amt] /* Sole Trader receiving PAYE deducted income */
	,[inc_tax_yr_sum_S02_tot_amt] /* Sole trader receiving withholding tax deducted income */
	,[inc_tax_yr_sum_S03_tot_amt] /* Rental income FROMIR3 */
	,[inc_tax_yr_sum_all_srces_tot_amt] AS [totinc]
	,CASE 
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] < 0
			THEN 'Annual income: Loss'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] = 0
			THEN 'Annual income: Zero'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 0
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 5000
			THEN 'Annual income: above 0 to 5000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 5000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 10000
			THEN 'Annual income: above 5000 to 10000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 10000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 15000
			THEN 'Annual income: above 10000 to 15000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 15000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 20000
			THEN 'Annual income: above 15000 to 20000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 20000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 25000
			THEN 'Annual income: above 20000 to 25000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 25000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 30000
			THEN 'Annual income: above 25000 to 30000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 30000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 35000
			THEN 'Annual income: above 30000 to 35000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 35000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 40000
			THEN 'Annual income: above 35000 to 40000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 40000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 50000
			THEN 'Annual income: above 40000 to 50000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 50000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 60000
			THEN 'Annual income: above 50000 to 60000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 60000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 70000
			THEN 'Annual income: above 60000 to 70000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 70000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 80000
			THEN 'Annual income: above 70000 to 80000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 80000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 90000
			THEN 'Annual income: above 80000 to 90000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 90000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 100000
			THEN 'Annual income: above 90000 to 100000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 100000
			THEN 'Annual income: above 100000'
		ELSE 'Annual income: unclassified'
		END AS [description]
FROM [$(IDIREF)].[data].[income_tax_yr_summary];
GO

/*
event view income 
based ON tax brackets 
*/
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)evtvw_totinc_year]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_totinc_year];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_totinc_year]
AS
SELECT [snz_uid]
	,DATEFROMPARTS([inc_tax_yr_sum_year_nbr], 1, 1) AS [start_date]
	,DATEFROMPARTS([inc_tax_yr_sum_year_nbr], 12, 31) AS [end_date]
	,CASE 
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] <= 20000
			THEN 'Annual income: 20,000 and below'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 20000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 48000
			THEN 'Annual income: above 20,000 to 48,000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 48000
			AND [inc_tax_yr_sum_all_srces_tot_amt] <= 70000
			THEN 'Annual income: above 48,000 to 70,000'
		WHEN [inc_tax_yr_sum_all_srces_tot_amt] > 70000
			THEN 'Annual income: above 70,000'
		ELSE 'Annual income: unclassified'
		END AS [description]
	,1 AS [value]
	,'IRD' AS [source]
FROM [$(IDIREF)].[data].[income_tax_yr_summary]
GO

/*reshaping partnership to match tax year */
IF OBJECT_ID('[IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)partnership_relationships_longyear]', 'U') IS NOT NULL
	DROP TABLE [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)partnership_relationships_longyear];
GO

WITH n (
	id
	,pid
	,yr
	,n0
	,n1
	)
AS (
	SELECT snz_uid
		,partner_snz_uid
		,datepart(year, [start_date]) AS [yr]
		,datepart(year, [start_date]) AS [yr0]
		,datepart(year, [end_date]) AS [yr1]
	FROM [IDI_Sandpit].[$(PROJSCH)].[ngt_partnership_relationships]
	
	UNION ALL
	
	SELECT id
		,pid
		,yr + 1
		,n0
		,n1
	FROM n
	WHERE yr < n1 --FROM the with
	)
SELECT id AS snz_uid
	,pid AS partner_snz_uid
	,yr
INTO [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)partnership_relationships_longyear]
FROM n
INNER JOIN [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)popyr_tak_unorm] [pop] ON [pop].[snz_uid] = [n].[id]
OPTION (MAXRECURSION 1000);
GO

/* Wages and salaries by tax year with partner weighted by duratiON of partnership*/
IF OBJECT_ID('[$(PROJSCH)].[$(TBLPREF)evtvw_partner_totinc_year]', 'V') IS NOT NULL
	DROP VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_partner_totinc_year];
GO

CREATE VIEW [$(PROJSCH)].[$(TBLPREF)evtvw_partner_totinc_year]
AS
SELECT [snz_uid]
	,DATEFROMPARTS([yr], 1, 1) AS [start_date]
	,DATEFROMPARTS([yr], 12, 31) AS [end_date]
	,CASE 
		WHEN [totinc] + [p_wt] * [p_totinc] <= 40000
			THEN 'Combined annual income: 40,000 and below'
		WHEN [totinc] + [p_wt] * [p_totinc] > 40000
			AND [totinc] + [p_wt] * [p_totinc] <= 80000
			THEN 'Combined annual income: above 40,000 to 80,000'
		WHEN [totinc] + [p_wt] * [p_totinc] > 80000
			AND [totinc] + [p_wt] * [p_totinc] <= 100000
			THEN 'Combined annual income: above 80,000 to 100,000'
		WHEN [totinc] + [p_wt] * [p_totinc] > 100000
			THEN 'Combined annual income: above 100,000'
		ELSE 'Combined annual income: unclassified'
		END AS [description]
	,1 AS [value]
	,'IRD, dia' AS [source]
FROM (
	SELECT [main_inc].*
		,[tax].[inc_tax_yr_sum_all_srces_tot_amt] AS [p_totinc]
		,cast(datediff(day, [p_inc_start], [p_inc_end]) AS FLOAT) / 365 AS [p_wt]
	FROM (
		SELECT [pop].*
			,[reln].[start_date]
			,[reln].[end_date]
			,Iif([reln].[start_date] > DATEFROMPARTS([pop].[yr], 1, 1), [reln].[start_date], DATEFROMPARTS([pop].[yr], 1, 1)) AS [p_inc_start]
			,Iif([reln].[end_date] < DATEFROMPARTS([pop].[yr], 12, 31), [reln].[end_date], DATEFROMPARTS([pop].[yr], 12, 31)) AS [p_inc_end]
			,[tax].[inc_tax_yr_sum_all_srces_tot_amt] AS [totinc]
		FROM [IDI_Sandpit].[$(PROJSCH)].[$(TBLPREF)partnership_relationships_longyear] [pop]
		INNER JOIN [IDI_Sandpit].[$(PROJSCH)].[ngt_partnership_relationships] [reln] ON [pop].[snz_uid] = [reln].[snz_uid]
			AND [pop].[partner_snz_uid] = [reln].[partner_snz_uid]
		INNER JOIN [$(IDIREF)].[data].[income_tax_yr_summary] [tax] ON [pop].[snz_uid] = [tax].[snz_uid]
			AND [pop].[yr] = [tax].[inc_tax_yr_sum_year_nbr]
		WHERE [yr] BETWEEN datepart(year, [start_date])
				AND datepart(year, [end_date])
		) main_inc
	INNER JOIN [$(IDIREF)].[data].[income_tax_yr_summary] [tax] ON [main_inc].[partner_snz_uid] = [tax].[snz_uid]
		AND [main_inc].[yr] = [tax].[inc_tax_yr_sum_year_nbr]
	) [totinc]
GO


