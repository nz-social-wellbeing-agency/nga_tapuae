##################################################################################################################
#' Nga Tapuwae Project - Stepping Stones and Maori Student Transitions
#' Social Wellbeing Agency
#' Ngai Tahu
#' 
#' Description: creates control file dynamically to some extent
#' 
#' Input: parameters age at start, age at end, pivotal age; these are usually decalred in master.R
#' 
#' Output: stage_details.csv
#' 
#' Author: Joel Bancolita
#' Reference: Akilesh Chokkanathapuram (TPP project)
#' 
#' Dependencies: lib files
#' 
#' History (reverse order):
#' 20200910: JB revisions
#' 20200610: JB initialise
#'#################################################################################################################

#IO folder
#io_folder<-'input'

#constants (in the master)
#age_at_ref<-23 #30 #age at reference year;
#journey_age0<-15 #age start
# age0<-15 #age start
# age1<-18 #age end
# journey_age_piv<-15 #pivotal age

journey_length<-age_at_ref-journey_age0 #in years
time_unit<-'day'
time_splice<-'13-week'

#define time splices
#enum time_unit_label
#int_lb<-c('one', 'two', 'three', 'four', 'five', 'six' )
time_unit_factors <- c(13)
time_unit_labels <- c('Quarter')
#time_unit_seq <- seq(4,0)
#time_unit_factor <- 7*time_unit_seq + ifelse(time_unit_seq>0,0,1)
tu_idx<-1
time_unit_label<-time_unit_labels[tu_idx]
time_unit_value<-time_unit_factors[tu_idx]*7
date_diff_unit<-'day'
time_unit_start<-0
time_unit_end<-journey_length*365

time_sequence <- seq(time_unit_start, time_unit_end)

#date_diff_to_start <- 
time_label<-seq(time_unit_start/time_unit_value, time_unit_end/time_unit_value)+1
ts_len<-length(time_label)
datediff_to_start<-(time_label-1)*(time_unit_value)
datediff_to_end<-(time_label)*time_unit_value-1


################################################################
datediff_to_start<-datediff_to_start[-ts_len]
datediff_to_end<-datediff_to_end[-ts_len]
time_label<-time_label[-ts_len]
ts_len<-ts_len-1
datediff_to_end[ts_len]<-time_unit_end
################################################################

control_file_df<-data.frame(subset=rep('timeline_event', ts_len),
                            stage_name_label=rep('journey', ts_len),
                            time_unit_label=rep(time_unit_label,ts_len),
                            time_label,
                            datediff_unit=rep(date_diff_unit, ts_len),
                            datediff_to_start,datediff_to_end)
#add ever
control_file_df$subset<-as.character(control_file_df$subset)
control_file_df$time_unit_label<-as.character(control_file_df$time_unit_label)
control_file_df$datediff_unit<-as.character(control_file_df$datediff_unit)
control_file_df<-rbind(control_file_df,c('ever','journey','Ever', 0, 'year',0,	journey_length))

#add future (3 years into end)
control_file_df<-rbind(control_file_df,c('future','journey','Future', 0, 'year',journey_length-2,	journey_length))

if (!file.exists(io_folder)) {
  dir.create(io_folder)
}
write.csv(control_file_df,paste0('./',io_folder,'/stage_details.csv'),row.names=F )
