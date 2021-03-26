##################################################################################################################
#' Nga Tapuwae Project - Stepping Stones and Maori Student Transitions
#' Social Wellbeing Agency
#' Ngai Tahu
#' 
#' Description: master script calling required codes
#' 
#' Input:
#'
#' Output:
#' 
#' Author: Joel Bancolita
#' Reference: Akilesh Chokkanathapuram (TPP project)
#' 
#' Dependencies: lib files
#' Check before starting: 
#'  -tblpref, schema, etc. parameters
#'  -event_date in paste0(tblpref, 'event_locations') correspond to the desired event, e.g. DOB at age 15
#'  -control files correspond to population to process
#' cluster plot done per batch   
#'   
#'  
#' History (reverse order):
#' 20200817: JB re-run and mods on new R
#' 20200810: JB revisions
#' 20200715: JB revise
#' 20200610: JB initialise
#'#################################################################################################################

#control file params
age_at_ref<-23 #30 #age at reference year; let's stop at 23 for now
journey_age0<-15 #age start

#.libPaths(c(.libPaths(),'/path/to/your/packrat/'))
setwd("/path/to/your/project/source")
#temporary: to remove all other errors due to updated version

# connection details
DEFAULT_SERVER = "SQL.Instance.Stats.NZ"
DEFAULT_DATABASE = "IDI_REFRESH"
DEFAULT_SCHEMA = "DATALAB_PROJECT_SCHEMA"
DEFAULT_PORT = PORTNUM

# user controls (from journey_output)
DEVELOPMENT_MODE = FALSE
CAUTIOUS_MODE = TRUE
SINGLE_LARGE_AREA_MODE = FALSE
# cluster settings
NUMBER_OF_CLUSTERS = 5
NUMBER_OF_BATCHES = 60 #7 in the NGT output
SEQUENCE_CHANNEL_BATCH_SIZE = 6 #half of the desc?
CLUSTER_PLOT = FALSE
CRIT_N = 8000 #number of tolerable n in batch processing

#Notes: changing at least one of: number batches, sequence channel batch size changes the composition of the clusters created
io_folder<-'input'
tblpref<-'swangtoa_'

# utility functions for SQL and db operations with packages such as dplyr and dbplyr
source('./utility_functions.R')

# modified script that extends control file
source('./control_file.R')

# methods, functions for executing journey timelines
source('./journey_timelines_functions.R')
source('./journey_timelines.R')

# functions for journey output
source('./journey_output_functions.R')

# representative timelines
source('./journey_output.R')

#save.image(paste0(tblpref,'RT_all',Sys.Date(),'.rdata') )




