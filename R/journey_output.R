#####################################################################################################
#' Description: Command script for producing Journey Timelines Output
#'
#' Input: SQL table produced by journey_timelines.R
#'
#' Output: 
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies:
#' - utility_functions.R
#' - journey_output_functions.R
#' 
#' Notes: Last runtime: 
#' (increase/change)
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2020-06-16 JB edits
#' 2019-01-17 SA v0
#####################################################################################################
#tblpref<-'swangt_'
#time_unit_label<-[specify, currently from control_file.R]
## source ----
# source('utility_functions.R')
# source('journey_output_functions.R')
library(tidyr)
library(data.table)

## parameters ----

# # user controls
# DEVELOPMENT_MODE = FALSE
# CAUTIOUS_MODE = TRUE
# SINGLE_LARGE_AREA_MODE = FALSE
# # cluster settings
# NUMBER_OF_CLUSTERS = 12
# NUMBER_OF_BATCHES = 2
# SEQUENCE_CHANNEL_BATCH_SIZE = 6
# CLUSTER_PLOT = TRUE

# input tables
EVENT_TABLE = paste0(tblpref,"event_locations")
GATHERED_DATA_TABLE = paste0(tblpref,"intersect_union")

# other
INPUT_COLUMNS = c("snz_uid", "role", "event_id", "stage_name", "time_unit", "time",
                  "description", "value", "source")
OUTPUT_COLUMNS = c("group", "group_size", "role", "stage_name", "time_unit", "time",
                   "description", "source", "value")

TIMELINE_COLUMNS = as.character(time_label) #JB: built from control file

## setup ----

run_time_inform_user("---- GRAND START ----")
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2020-35]"
our_views = "[IDI_UserCode].[DL-MAA2020-35]"

## load event tables ----

run_time_inform_user("create access points")

events = create_access_point(db_connection = db_con_IDI_sandpit,
                             schema =  our_views,
                             tbl_name = EVENT_TABLE,
                             db_hack = TRUE) %>%
  group_by(event_id,event_date) %>% tally()
  
 
required_columns = c("event_id", "event_date")
assert(table_contains_required_columns(events, required_columns, only = FALSE),
       msg = "event table does not contain all required columns")

if(DEVELOPMENT_MODE){
  events = events %>% filter(floor(event_id / 100) %% 100 == 0)
  NUMBER_OF_CLUSTERS = 5
  NUMBER_OF_BATCHES = 10
}

gathered_data = create_access_point(db_connection = db_con_IDI_sandpit,
                                    schema = our_schema,
                                    tbl_name = GATHERED_DATA_TABLE)
assert(table_contains_required_columns(gathered_data, INPUT_COLUMNS, only = FALSE),
       msg = "gathered data does not contain all required columns")

run_time_inform_user("load non-journey into R")

non_journey = gathered_data %>%
  filter(time_unit != time_unit_label) %>%
  inner_join(events, by = "event_id") %>%
  mutate(the_year = YEAR(event_date)) %>%
  collect()

run_time_inform_user("load journey into R")

journey = gathered_data %>%
  filter(time_unit == time_unit_label) %>%
  inner_join(events, by = "event_id") %>%
  mutate(the_year = YEAR(event_date)) %>%
  collect()

# ad hoc adjustment of birth weights to categorical
is_birth_weight = non_journey$description == 'BIRTH WEIGHT'
non_journey[is_birth_weight,"value"] = 200 * round(non_journey[is_birth_weight,"value"]  / 200, 0)

## collapse multiple siblings into single identity ----

siblings = journey %>%
  filter(role %in% c('full sibling', 'half sibling')) %>%
  group_by(role, event_id, stage_name, time_unit, time, description, source, the_year) %>%
  summarise(value = sum(value))

non_siblings = journey %>%
  filter(role %not_in% c('full sibling', 'half sibling')) %>%
  select(colnames(siblings))

journey_prepared = rbind(data.table(siblings),
                       data.table(non_siblings),
                       use.names = TRUE)

## group pivot-journeys by clustering ----
# Integrating Michael's work

run_time_inform_user("clustering")

pivot_journey = journey_prepared %>%
  mutate(triplet = sprintf("%s-%s-%s",role,source,description)) %>%
  select(event_id, triplet, time, value) %>%
  mutate(value = pmin(value, 1)) %>%
  spread(time, value, fill = 0) %>%
  as.data.frame()

# events to cluster
events_to_cluster = pivot_journey %>% select(event_id) %>% unique()
# role, source, description triplets to cluster
triplets_to_cluster = pivot_journey %>% select(triplet) %>% unique()

if(!SINGLE_LARGE_AREA_MODE) {
  event_clusters = batch_processing(
    pivot_journey,
    "event_id",
    "triplet",
    channel_batch_size = SEQUENCE_CHANNEL_BATCH_SIZE,
    number_of_clusters = NUMBER_OF_CLUSTERS,
    give_plots = CLUSTER_PLOT,
    number_of_batches = NUMBER_OF_BATCHES
  )

  # convert medoids to text strings
  medoids = event_clusters %>% select(medoid) %>% unique()
  medoids = medoids %>% mutate(group_name = paste0("cluster ",1:nrow(medoids)))
  event_clusters = event_clusters %>%
    inner_join(medoids, by = "medoid") %>%
    select(event_id, group_name)
}

## group non-journeys by manual definition ----
run_time_inform_user("manual group definition")

### marginal / all
all_grp<-non_journey %>% select(event_id) %>%
  unique() %>%
  mutate(group_name = 'All groups')

### marginal / one-by groups
#### income is later 3 years before end: journey_prepared %>% filter(time>24,description=='Annual income: 20,000 and below') %>% select(event_id) %>% unique() %>% tally()
grp.df<-non_journey %>% group_by(source, description, time) %>% tally()
grp.df.coll<-data.frame()
for (i in 1:nrow(grp.df) ) {
  tmp.df <- non_journey %>%
    filter(source==grp.df$source[i], 
           time==grp.df$time[i], description==grp.df$description[i]) %>%
    select(event_id) %>%
    unique() %>%
    mutate(group_name = paste0(grp.df$source[i], ':',grp.df$description[i]))
  
  grp.df.coll<-rbind(grp.df.coll,tmp.df)
  
}

### biological parents : sex
female_parents <- non_journey %>%
  filter(time==0, description=='Sex = Female') %>%
  select(event_id) %>% unique() %>%
  inner_join(
    non_journey  %>%
      filter(time==0, description=='Became a parent') %>%
      select(event_id) %>% unique(), by='event_id'
    )%>%
  mutate(group_name = 'Female parent')

male_parents <- non_journey %>%
    filter(time==0, description=='Sex = Male') %>%
    select(event_id) %>% unique() %>%
    inner_join(
      non_journey  %>%
        filter(time==0, description=='Became a parent') %>%
        select(event_id) %>% unique(), by='event_id' 
    )%>%
  mutate(group_name = 'Male parent')

### non-parents (over the journey): sex
female_never_parents <- non_journey %>%
  filter(time==0, description=='Sex = Female') %>%
  select(event_id) %>% unique() %>%
  anti_join(
    non_journey  %>%
      filter(time==0, description=='Became a parent') %>%
      select(event_id) %>% unique(), by='event_id' 
  )%>%
  mutate(group_name = 'Female never became parent')

male_never_parents <- non_journey %>%
  filter(time==0, description=='Sex = Male') %>%
  select(event_id) %>% unique() %>%
  anti_join(
    non_journey  %>%
      filter(time==0, description=='Became a parent') %>%
      select(event_id) %>% unique(), by='event_id' 
  )%>%
  mutate(group_name = 'Male never became parent')

### attended level 7 after age 20 
post20_lvl7 <- journey %>%
  filter(time>20, description %like% 'stage tertiary') %>%
  select(event_id) %>% unique() %>%
  mutate(group_name = 'Attended tertiary after age 20')

### attended level 7 after age 20 
not_suspended_stooddown <- non_journey %>% filter(time==0,description %like% 'Intervention') %>%# group_by(description) %>% tally()
  select(event_id) %>% unique() %>%
  anti_join(
    non_journey %>%
      filter(time==0,description %like% 'Suspensions' | description %like% 'Stand down') %>% 
      select(event_id) %>% unique(), by='event_id'
    ) %>%
  mutate(group_name = 'Never suspended/stood-down')

## consolodate groups ----

if(!SINGLE_LARGE_AREA_MODE) {
  # gather all constructed group
  all_groups = rbind(data.table(event_clusters),
                     data.table(all_grp),
                     data.table(grp.df.coll),
                     data.table(female_parents),
                     data.table(male_parents),
                     data.table(female_never_parents),
                     data.table(male_never_parents),
                     data.table(post20_lvl7),
                     data.table(not_suspended_stooddown),
                     use.names = TRUE
                     )
}
# remove unneeded & large datasets
if(! DEVELOPMENT_MODE)
  rm("alcohol_drug", "b4sc_declined", "b4sc_need", "b4sc_pass", "b4sc_declined_harsh",
     "certificate_qualificiation", "chronic_condition",
     "ethnic_asian", "ethnic_european", "ethnic_maori", "ethnic_other",
     "ethnic_pacific", "first_pregnancies", "graduate_qualificiation", "hard_birth", "hard_pregnancy",
     "hard_to_term", "low_birth_weight", "maternal_MH", "maternal_tobacoo_concern", "new_migrants",
     "no_qualificiation", "older_mother", "postgrad_qualificiation", "prev_sentence", "teen_mother",
     "local_board", "asian_first_birth", "european_first_birth", "maori_first_birth", "pacifica_first_birth")

# discard groupsif only wanting a singlelarge area
if(SINGLE_LARGE_AREA_MODE)
  all_groups = all_events

# write to SQL
output_cols = list(event_id = "[int] NOT NULL",
                   group_name = "[varchar](50) NOT NULL")
tmp = copy_r_to_sql(db_con_IDI_sandpit, our_schema, paste0(tblpref,"events_by_group"), as.tbl(all_groups), OVERWRITE = TRUE)


## summary of non-journey ----

run_time_inform_user("output non-journey results")

# totals
totals_results = non_journey %>%
  inner_join(all_groups, by = "event_id") %>%
  group_by(group_name, role, stage_name, time_unit, time, source, description) %>%
  summarise(num_events = n_distinct(event_id),
            num_people = n_distinct(snz_uid),
            total_value = sum(as.numeric(value)))
   
# histogram --- unused

histogram_results = non_journey %>%
  inner_join(all_groups, by = "event_id") %>%
  filter(description %in% c("ED visit", "exposed to family violence", "Mothers age at birth",
                            "non-enrolled PHO contact", "number of concerns at birth",
                            "number of concerns during pregnancy", "previous completed pregnancies",
                            "previous pregnancies", "BIRTH WEIGHT")) %>%
  group_by(group_name, role, stage_name, time_unit, time, source, description, value) %>%
  summarise(num_events = n_distinct(event_id),
            num_people = n_distinct(snz_uid),
            num_w_value = n())

# write output
write.csv(totals_results, file = paste0(tblpref,"totals_results.csv"), row.names = FALSE)
write.csv(histogram_results, file = paste0(tblpref,"histogram_results.csv"), row.names = FALSE)

## summary of journey ----
# Integrating Athira's work

run_time_inform_user("output journey results")

# get list of unique groups
group_size = all_groups %>%
  group_by(group_name) %>%
  summarise(num = n())
group_list = group_size[,1]

output_representative_timelines = data.frame(stringsAsFactors = FALSE)

# iterate through each group
for(jj in 1:nrow(group_size)){
  # inform user
  run_time_inform_user(sprintf("-- group: %d", jj))
  # extract group
  this_group = all_groups %>%
    filter(group_name == group_size$group_name[jj]) %>%
    inner_join(journey_prepared, by = "event_id", suffix = c("_g",""))
  
  # list of journeys
  unique_journeys = this_group %>%
    select(group_name, role, source, description) %>%
    unique()
  
  # for each unique journey
  for(ii in 1:nrow(unique_journeys)){
    # select records to make representative
    this_pivot_journey = this_group %>%
      filter(group_name == unique_journeys$group_name[ii],
             role == unique_journeys$role[ii],
             source == unique_journeys$source[ii],
             description == unique_journeys$description[ii]) %>%
      # select(time, value) %>%
      mutate(value = pmin(value, 1)) %>%
      spread(time, value, fill = 0)
    
    # just columns of journey
    this_pivot_journey = this_pivot_journey %>%
      select(TIMELINE_COLUMNS[TIMELINE_COLUMNS %in% colnames(this_pivot_journey)]) %>%
      as.data.frame()
    
    # ensure all required columns are present
    missing_cols = setdiff(TIMELINE_COLUMNS, names(this_pivot_journey))
    this_pivot_journey[missing_cols] = 0
    this_pivot_journey = this_pivot_journey[TIMELINE_COLUMNS]
        
    # number of timeline events in representative timeline
    num_representative_events = round(sum(this_pivot_journey) / nrow(this_pivot_journey))
    
    if(num_representative_events >= 1){
      # make representative timeline
      representative_timeline = make_representative_timeline(this_pivot_journey,
                                                             num_representative_events,
                                                             TIMELINE_COLUMNS)
      
      # add key details
      representative_timeline = representative_timeline %>%
        mutate(group_name = unique_journeys$group_name[ii],
               role = unique_journeys$role[ii],
               source = unique_journeys$source[ii],
               description = unique_journeys$description[ii],
               num_contributing_indiv = nrow(this_pivot_journey),
               group_size = group_size$num[jj])
      
      # append to dataset
      output_representative_timelines = rbind(output_representative_timelines, 
                                               representative_timeline)
    }
  }
}

# write output
write.csv(output_representative_timelines, file = paste0(tblpref,"representative_timelines.csv"), row.names = FALSE)

## conclude ----


run_time_inform_user("---- GRAND END ----")



