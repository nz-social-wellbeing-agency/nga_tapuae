# Nga Tapuae 
Collection of project-specific codes used to build tables and views in the Stats NZ Integrated Data Infrastructure (IDI) and apply the Representative Timelines methodology.

# Overview

Social Wellbeing Agency (SWA) has been working in partnership with Ngāi Tahu iwi to conduct data discovery and to learn actionable insights that identify the most important barriers, levers, and boosters that help young Māori to succeed. We named this research Ngā Tapue (stepping stones) to emphasise the search for positive and hopeful journeys for rangatahi. The  Agency proposed to analyse sequences of social events, such as education, training, employment and summarise similar experiences by replicating the Representative Timelines Methodology used in the Having a Baby in South Auckland (HaBiSA) project

# Reference material

This repository provides the code for building dataset and customisations for the Nga Tapuae project. It is highly recommended that the user read the Agency's publications Representative timelines – modelling people’s life experiences and forthcoming [Nga Tapuae Technical Report and Mixed Methods Report].
This code also utilises other measures in the [Social Wellbeing Agency measures library](https://github.com/nz-social-wellbeing-agency/definitions_library), hence this will refer to existing ones.

# Installation

To install the tool, download this repository, copy it to your working location and unzip it. There are then two main sets of codes, i.e. SQL to build the tables and views and customised R codes to apply representative timelines method for this project. Hence, the user can run:
1.	Master.sql using SQL Management Studio 
2.	Master.R using R Studio
All SQL scripts contain descriptions and dependencies if applicable. We tried parameterisation to attempt replicability where parameters can be set in master.sql for clarity.  For instance, master.sql has AGE, AGE0 and AGE1 parameters to set reference age, and population start and end age respectively.  There are also parameters to set location of the population. Setting command-line parameters is not enabled by default, hence users need to activate this by clicking **Query->SQLCMD** Mode. Finally, each SQL has commented-out parameters which can be used to run individually.
Master.R incorporates changes such as in time window and tackling changes due to package updates. The codes have been tested to work on R 3.6.3, the version of R server as of writing in the IDI, together with packages installed.

## Citation

Social Wellbeing Agency (2021). Nga Tapuae. Source code. https://github.com/nz-social-wellbeing-agency/nga_tapuae

# Getting Help
General enquiries can be sent to info@swa.govt.nz.  

