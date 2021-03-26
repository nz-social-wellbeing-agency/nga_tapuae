/**************************************************************************************************************
Title: Nga Tapuae specific SQL master script
Author: Joel Bancolita
		Social Wellbeing Agency

Intended use: 
Just a master script to call all scripts in sequence

History (reverse order): 
20210228: JB initialise


Depends:	
- see SQLs
**************************************************************************************************************/
--PARAMETERS##################################################################################################
--SQLCMD only (Activate by clicking Query->SQLCMD Mode)
:setvar TBLPREF "swangt_" 
:setvar IDIREF "IDI_Clean_20200120" 
:setvar PROJSCH "DL-MAA2020-35"

--path to data folder
:setvar DATPATH "path/to/your/data/repo" /* e.g. "\\prtprdsasnas01\datalab\maa\MAA2020-35\nga_tapuwae-src\data" */

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

--reference year and birth year
:setvar SQLREPO "path/to/your/SQL/repo" /* e.g. I:\MAAxxxx-xx\projectA\src\sql */
GO
--############################################################################################################

print 'population definition'
:r $(SQLREPO)"\Pop definitions\popdefn-stage.sql"

print 'demographics and address decriptors'
:r $(SQLREPO)"\Demographics\addr-desc-stage.sql"
:r $(SQLREPO)"\Demographics\ethnicity-indicator-stage.sql"
:r $(SQLREPO)"\Demographics\sex-indicator-stage.sql"

print 'cyf events'
:r $(SQLREPO)"\CYF events\cyf-events-stage-MB.sql"

print 'employment and income'
:r $(SQLREPO)"\Employment\employment-stage.sql"
:r $(SQLREPO)"\Employment\income-stage.sql"

print 'education and related events'
:r $(SQLREPO)"\Education\assess-standards-stage.sql"
:r $(SQLREPO)"\Education\events-stage.sql"
:r $(SQLREPO)"\Education\msd-events-stage.sql"

print 'family related events'
:r $(SQLREPO)"\Family\older_sibling_qualification-stage.sql"
:r $(SQLREPO)"\Family\parent-char-stage.sql"

print 'health'
:r $(SQLREPO)"\Health\moh-events-stage.sql"

print 'life events'
:r $(SQLREPO)"\Life events\fv-events-stage.sql"
:r $(SQLREPO)"\Life events\high_deprivation.sql"
:r $(SQLREPO)"\Life events\nzta-reg-stage.sql"
:r $(SQLREPO)"\Life events\social_housing-events-stage.sql"

print 'moj events'
:r $(SQLREPO)"\MoJ events\corr-events-stage.sql"
:r $(SQLREPO)"\MoJ events\moj-events-stage.sql"


--##############################################################################################################
