##################################################################################################################
#' Description: Analysis functions for producing Journey Timelines
#'
#' Input: As per each function
#'
#' Output: Functions in R environment for use in analysis
#' 
#' Author: Simon Anastasiadis
#' 
#' QA: 20190424 Akilesh Chokkanathapuram
#' 
#' Dependencies:
#' - utility_functions.R (already sourced into environment)
#' 
#' Notes:
#' 
#' Issues:
#' 
#' History (reverse order):
#'  2020-08-20 JB update to fix suffix error
#'  2020-06-15 JB tblpref
#'  2019-04-24 AK QA
#'  2018-12-20 SA v1
#'  2018-12-10 SA v0
#'
#'#################################################################################################################

#' Produces table describing the event stage
#' 
#' Event Stages is an SQL table/view with the following required columns:
#' snz_uid, role, event_id, start_date, end_date, stage_name, time_unit, time
#'
#' Input table 1: "event with roles" with the required columns: snz_uid, role, event_id
#' Input table 2: "event with date" with the required columns: event_id, event_date
#' Input table 3: control table defining stages, with the required columns:
#' stage_name_label, time_unit_label, time_label, datediff_unit, datediff_to_start, datediff_to_end
#'
#'

#add tbl prefix
#tblpref<-'swangt_'

produce_event_stages = function(db_connection, schema, output_table_name, event_w_roles, event_w_date, controls){
  ## validation ----
  # input types
  assert(is.data.frame(event_w_roles) | is.tbl(event_w_roles), "event_w_roles must be a tbl or dataframe")
  assert(is.data.frame(event_w_date) | is.tbl(event_w_date), "event_w_date must be a tbl or dataframe")
  assert(is.data.frame(controls) | is.tbl(controls), "controls must be a tbl or dataframe")
  # columns names
  assert(table_contains_required_columns(event_w_roles, required_columns = c("snz_uid", "role", "event_id")),
         "event_w_roles does not have required columns")
  assert(table_contains_required_columns(event_w_date, required_columns = c("event_id", "event_date")),
         "event_w_date does not have required columns")
  required_columns = c("stage_name_label", "time_unit_label", "time_label", "datediff_unit",
                       "datediff_to_start", "datediff_to_end")
  assert(table_contains_required_columns(controls, required_columns), "controls does not have required columns")
  
  ## create table ----
  
  event_stage_table_columns = list(snz_uid = "[int] NOT NULL",
                                   role = "[varchar](20) NOT NULL",
                                   event_id = "[int] NOT NULL",
                                   start_date = "[date] NOT NULL",
                                   end_date = "[date] NOT NULL",
                                   stage_name = "[varchar](25) NOT NULL",
                                   time_unit = "[varchar](25) NOT NULL",
                                   time = "[int] NOT NULL",
                                   subset = "[varchar](25) NOT NULL")
  
  event_stages = create_table(db_connection, schema, tbl_name = output_table_name,
                                 named_list_of_columns = event_stage_table_columns, OVERWRITE = TRUE)
  
  ## assemble ----
  
  for(row_num in 1:nrow(controls)){
    event_stage = produce_event_stages_single_row(event_w_roles, event_w_date, controls, row_num)
    
    append_database_table(db_connection, schema, paste0(tblpref,"tmp_stage_events"), 
                          names(event_stage_table_columns), event_stage)
  }
    
  ## output ----
  
  result = create_clustered_index(db_connection, schema, paste0(tblpref,"tmp_stage_events"), "snz_uid")
  
  required_columns = c("snz_uid", "role", "event_id", "start_date", "end_date", "stage_name", "time_unit", "time")
  event_stages = create_access_point(db_connection, schema, tbl_name = paste0(tblpref,"tmp_stage_events")) %>%
    select(required_columns)
  
  return(event_stages)
}

#' Produces tables describing the event stage (inner loop)
#' dynamically in response to a specified row of the control table
#' 
#' Event Stages is an SQL table/view with the following required columns:
#' snz_uid, role, event_id, start_date, end_date, stage_name, time_unit, time
#'
#' Input table 1: "event with roles" with the required columns: snz_uid, role, event_id
#' Input table 2: "event with date" with the required columns: event_id, event_date
#' Input table 3: control table defining stages, with the required columns:
#' stage_name_label, time_unit_label, time_label, datediff_unit, datediff_to_start, datediff_to_end
#'
#'
produce_event_stages_single_row = function(event_w_roles, event_w_date, controls, row_num){
  ## validation ----
  # input types
  assert(is.data.frame(event_w_roles) | is.tbl(event_w_roles), "event_w_roles must be a tbl or dataframe")
  assert(is.data.frame(event_w_date) | is.tbl(event_w_date), "event_w_date must be a tbl or dataframe")
  assert(is.data.frame(controls) | is.tbl(controls), "controls must be a tbl or dataframe")
  # columns names
  assert(table_contains_required_columns(event_w_roles, required_columns = c("snz_uid", "role", "event_id")),
         "event_w_roles does not have required columns")
  assert(table_contains_required_columns(event_w_date, required_columns = c("event_id", "event_date")),
         "event_w_date does not have required columns")
  required_columns = c("stage_name_label", "time_unit_label", "time_label", "datediff_unit",
                       "datediff_to_start", "datediff_to_end")
  assert(table_contains_required_columns(controls, required_columns), "controls does not have required columns")
  # row number within control table
  assert(1 <= row_num & row_num <= nrow(controls), "requested row number out of scope")
  
  ## extract values ----
  
  datediff_unit = controls$datediff_unit[row_num]
  datediff_to_start = controls$datediff_to_start[row_num]
  datediff_to_end = controls$datediff_to_end[row_num]
  stage_name_label = controls$stage_name_label[row_num]
  time_unit_label = controls$time_unit_label[row_num]
  time_label = controls$time_label[row_num]
  subset_label = controls$subset[row_num]
  
  ## assemble ----
  
  event_stage = event_w_roles %>%
    inner_join(event_w_date, by = "event_id") %>%
    mutate(start_date = DATEADD(sql(!!enquo(datediff_unit)), !!enquo(datediff_to_start), event_date),
           end_date = DATEADD(sql(!!enquo(datediff_unit)), !!enquo(datediff_to_end), event_date),
           stage_name = !!enquo(stage_name_label),
           time_unit =!!enquo(time_unit_label),
           time = !!enquo(time_label),
           subset = !!enquo(subset_label))
  
  ## output ----
  
  required_columns = c("snz_uid", "role", "event_id", "start_date", "end_date",
                       "stage_name", "time_unit", "time", "subset")
  event_stage = event_stage %>% select(required_columns)
  
  return(event_stage)
}

#' Compact spells with close end and start dates
#' 
#' Where two spells have an end_date and a start_date that are within overlap_days of each
#' other, we compact the spells into a single spell. Other columns are aggregated as described.
#'
#' Note: currently uses an SQL hack to pass a list to summarise to dbplyr. This requires refinement
#' for use with plain R data.frames.
#'
#' Inspired by SAS script si_compact_spells by Vinay Benny
#' Following methodology in SQL script *_spells_non_overlapping by Simon A
#'
compact_spells = function(tbl_name, within, start_date_col, end_date_col, overlap_days,
                          db_connection, schema, ...,
                          cols_to_max = c(), cols_to_min = c(), cols_to_sum = c(),
                          cols_to_avg = c(), cols_to_count = c()){
  ## validation ----
  # cols_to_* must be named
  assert(length(list(...)) == 0, "cols_to_* arguments must be named")
  # input types
  assert(is.tbl(tbl_name) | is.data.frame(tbl_name), "table must be a tbl or a data.frame")
  assert(is.numeric(overlap_days) & 0 <= overlap_days, "overlap_days must be numeric and non-negative")
  assert(all(sapply(within, is.character)), "within must contain entries of type character")
  assert(all(sapply(start_date_col, is.character)), "start_date_col must contain entries of type character")
  assert(all(sapply(end_date_col, is.character)), "end_date_col must contain entries of type character")
  assert(all(sapply(cols_to_max, is.character)), "cols_to_max must contain entries of type character")
  assert(all(sapply(cols_to_min, is.character)), "cols_to_min must contain entries of type character")
  assert(all(sapply(cols_to_sum, is.character)), "cols_to_sum must contain entries of type character")
  assert(all(sapply(cols_to_avg, is.character)), "cols_to_avg must contain entries of type character")
  assert(all(sapply(cols_to_count, is.character)), "cols_to_count must contain entries of type character")
  # dates are singletons
  assert(length(start_date_col) == 1, "start date must be a single column name")
  assert(length(end_date_col) == 1, "end date must be a single column name")
  # variable in table
  for(col in c(within,start_date_col,end_date_col,cols_to_max,cols_to_min,cols_to_sum,cols_to_avg,cols_to_avg))
    assert(col %in% colnames(tbl_name), paste0(col, " not in table"))
  
  ## markers of groups to merge ----
  sort_order = c(within, start_date_col, end_date_col)
  
  marked_for_merge = tbl_name %>%
    mutate(next_start_sqkgypt = lead(!!sym(start_date_col), 1, order_by = sort_order),
           next_end_sqkgypt = lead(!!sym(end_date_col), 1, order_by = sort_order)) %>%
    mutate(next_start_sqkgypt = ifelse(YEAR(next_start_sqkgypt) == 0000,
                                       DATEADD(YEAR, 1, next_start_sqkgypt), next_start_sqkgypt),
           next_end_sqkgypt = ifelse(YEAR(next_end_sqkgypt) == 9999,
                                     DATEADD(YEAR, -1, next_end_sqkgypt), next_end_sqkgypt)) %>%
    mutate(next_start_sqkgypt = DATEADD(DAY, -1 * !!enquo(overlap_days), next_start_sqkgypt),
           next_end_sqkgypt = DATEADD(DAY, !!enquo(overlap_days), next_end_sqkgypt)) %>%
    mutate(do_not_merge_w_next = ifelse(!!sym(start_date_col) <= next_end_sqkgypt
                                        & next_start_sqkgypt <= !!sym(end_date_col), 0, 1))
  
  cumsum_sql = paste0("SUM(do_not_merge_w_next) OVER( ORDER BY ",
                      "\"",paste(sort_order, collapse = "\", \""),"\"",
                      " ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)")
  
  marked_for_merge = marked_for_merge %>%
    mutate(group_sqkgypt = sql(cumsum_sql))
  
  marked_for_merge = write_for_reuse(db_connection, schema, paste0(tblpref,"tmp_marked_for_merge"), marked_for_merge)
  
  ## build summary components ----
  
  zz = quos(!!sym(start_date_col) := min(!!sym(start_date_col), na.rm = TRUE))
  zz = c(zz, quos(!!sym(end_date_col) := max(!!sym(end_date_col), na.rm = TRUE)))
  
  for(var in cols_to_max)
    zz = c(zz, quos(!!sym(var) := max(!!sym(var), na.rm = TRUE)))
  for(var in cols_to_min)
    zz = c(zz, quos(!!sym(var) := min(!!sym(var), na.rm = TRUE)))
  for(var in cols_to_avg)
    zz = c(zz, quos(!!sym(var) := mean(!!sym(var), na.rm = TRUE)))
  for(var in cols_to_sum)
    zz = c(zz, quos(!!sym(var) := sum(!!sym(var), na.rm = TRUE)))
  for(var in cols_to_count)
    zz = c(zz, quos(!!sym(var) := n(!!sym(var), na.rm = TRUE)))
  
  ## summarise overlapping spells ----
  groupers = c(within, "group_sqkgypt")
  
  compacted_spells = marked_for_merge %>%
    group_by(!!!syms(groupers)) %>%
    summarise(!!!zz)
  
  ## output ----
  
  required_columns = c(within, start_date_col, end_date_col,
                       cols_to_max, cols_to_min, cols_to_sum, cols_to_avg, cols_to_avg)
  compacted_spells = compacted_spells %>% select(!!!enquo(required_columns))
  
  return(compacted_spells)
}

#' Combine spells with overlapping start and end dates
#' 
#' Where two spells overlap we create a composite spell that has both their properties.
#' And we also craete spells for the non-overlapping pieces.
#' 
#' Assumes events in each table are non-overlapping (within the same table).
#' 
#' Insipred by SAS script si_combine_spells by Vinay Benny & Ben Vandenbroucke.
#'
combine_spells = function(tbl_name_1, tbl_name_2, within, start_date_col, end_date_col,
                          db_connection, schema, ...,
                          cols_to_duplicate = c(), cols_to_proportion = c()){
  ##
  ## start_date_col = "start_date_0"
  ## end_date_col = "end_date_0"
  
  ##combine_spells(this_event_stage, this_measure, within = "snz_uid",
                 # start_date_col = "start_date", end_date_col = "end_date",
                 # db_con_IDI_sandpit, our_schema,
                 # cols_to_duplicate = cols_to_duplicate,
                 # cols_to_proportion = cols_to_proportion)
  ##
  ## validation ----
  # cols_to_* must be named
  assert(length(list(...)) == 0, "cols_to_* arguments must be named")
  # input types
  assert(is.tbl(tbl_name_1) | is.data.frame(tbl_name_1), "table 1 must be a tbl or a data.frame")
  assert(is.tbl(tbl_name_2) | is.data.frame(tbl_name_2), "table 2 must be a tbl or a data.frame")
  assert(all(sapply(within, is.character)), "within must contain entries of type character")
  assert(all(sapply(start_date_col, is.character)), "start_date_col must be of type character")
  assert(all(sapply(end_date_col, is.character)), "end_date_col must be of type character")
  assert(all(sapply(cols_to_duplicate, is.character)), "cols_to_duplicate must be of type character")
  assert(all(sapply(cols_to_proportion, is.character)), "cols_to_proportion must be of type character")
  # dates are singletons
  assert(length(start_date_col) == 1, "start date must be a single column name")
  assert(length(end_date_col) == 1, "end date must be a single column name")
  # variable in table
  for(col in c(within,start_date_col,end_date_col))
    assert(col %in% colnames(tbl_name_1), paste0(col, " not in table 1"))
  for(col in c(within,start_date_col,end_date_col))
    assert(col %in% colnames(tbl_name_2), paste0(col, " not in table 2"))
  
  all_cols_to_ = c(cols_to_duplicate, cols_to_proportion)
  
  for(col in unique(all_cols_to_))
    assert(col %in% c(colnames(tbl_name_1), colnames(tbl_name_2)), paste0(col," does not appear in either table"))
  for(col in unique(all_cols_to_))
    assert(col %not_in% colnames(tbl_name_1) | col %not_in% colnames(tbl_name_2), 
           paste0(col," appears in more than one col_to_*"))
  assert(length(unique(all_cols_to_)) == length(all_cols_to_), "duplication not permitted within cols_to_*")
         
  
  
  ## core list of dates ----
  #
  # Requires a sort order:
  # sort by date first, and for start/end dates on the same day ensure the start dates come first.
  # 
  # Requires 4 rules to handle events being inclusive of their start and end dates:
  # 1) if new spell is start date to start date then shorten end by 1
  # 2) if new spell is end date to end date then shorten start by 1
  # 3) if new spell is end date to start date then shorten both by 1
  # 4) if new spell is start date to end date then leave both unchanged
  # Remove any new spells with length < 1
  
  tbl1_start_dates = tbl_name_1 %>% select(c(within, key_date = !!enquo(start_date_col))) %>% mutate(is_end = 0)
  tbl1_end_dates = tbl_name_1 %>% select(c(within, key_date = !!enquo(end_date_col))) %>% mutate(is_end = 1)
  tbl2_start_dates = tbl_name_2 %>% select(c(within, key_date = !!enquo(start_date_col))) %>% mutate(is_end = 0)
  tbl2_end_dates = tbl_name_2 %>% select(c(within, key_date = !!enquo(end_date_col))) %>% mutate(is_end = 1)
  
  required_columns = c(within, "key_date", "is_end")
  core_list_of_dates = tbl1_start_dates %>%
    union_all(tbl1_end_dates, required_columns) %>%
    union_all(tbl2_start_dates, required_columns) %>%
    union_all(tbl2_end_dates, required_columns) %>%
    mutate(key_date = ifelse(YEAR(key_date) == 9999, DATEADD(DAY, -30, key_date), key_date))
  
  core_list_of_dates = write_for_reuse(db_connection, schema, paste0(tblpref,"tmp_core_list_of_dates"),
                                       core_list_of_dates, index_columns = within)
  
  # give every record a start and end date labelled as per the input tables
  core_list_of_dates = core_list_of_dates %>%
    rename(!!sym(start_date_col) := key_date) %>%
    group_by(!!!syms(within)) %>%
    mutate(!!sym(end_date_col) := lead(!!sym(start_date_col), 1,
                                       order_by = c(!!enquo(start_date_col), "is_end") ),
           is_end_lead = lead(is_end, 1,
                                       order_by = c(!!enquo(start_date_col), "is_end") ) ) %>%
    filter(!is.na(!!sym(end_date_col))) %>%
    mutate(!!sym(end_date_col) := ifelse(is_end_lead == 0,
                                         DATEADD(DAY, -1, !!sym(end_date_col)),
                                         !!sym(end_date_col) ),
           !!sym(start_date_col) := ifelse(is_end == 1,
                                           DATEADD(DAY, 1, !!sym(start_date_col)),
                                           !!sym(start_date_col) )) %>%
    filter(!!sym(start_date_col) <= !!sym(end_date_col)) %>%
    distinct()
  
  ## build summary components ----
  
  zz = c()
  for(col in cols_to_proportion){
    # prefix for tbl1 or tbl2
    prefix = ifelse(col %in% colnames(tbl_name_1), "_1", "_2")
    sdate = paste0(start_date_col,prefix)
    edate = paste0(end_date_col,prefix)
    
    zz = c(zz, quos(!!sym(col) := !!sym(col) 
                    * DATEDIFF(DAY, !!sym(start_date_col), !!sym(end_date_col))
                    / (1 + DATEDIFF(DAY, !!sym(sdate), !!sym(edate)) )
    ))

  }

  ## full join & assembly ----
  ## 26-08-2020: SA added this to address error during SQL creation where snz_uid are duplicated
  tbl_name_1<- tbl_name_1 %>% ungroup()
  tbl_name_2<- tbl_name_2 %>% ungroup()
  ####
  
  
  #JB: Workaround for suffix not working####################################
  #####################################
  start_date_col_bak = start_date_col
  end_date_col_bak = end_date_col
  
  start_date_col = "start_date_0"
  end_date_col = "end_date_0"
  
  #####################################

  start_date_1 = paste0(start_date_col_bak,"_1")
  start_date_2 = paste0(start_date_col_bak,"_2")
  end_date_1 = paste0(end_date_col_bak,"_1")
  end_date_2 = paste0(end_date_col_bak,"_2")
  #####################################
  #######################################
  
  core_1 = core_list_of_dates %>%
    ungroup() %>% ## 26-08-2020 
    inner_join(tbl_name_1, by = within, suffix = c("_0","_1")) %>% #suffix was changed from c("","_1") to c("_0","_1"); new dbplyr seems to don't like having no argument 
    filter(!!sym(start_date_col) <= !!sym(end_date_1)
           & !!sym(start_date_1) <= !!sym(end_date_col))
  
  core_2 = core_list_of_dates %>%
    ungroup() %>% ## 26-08-2020
    inner_join(tbl_name_2, by = within, suffix = c("_0","_2")) %>% #suffix was changed from c("","_1") to c("_0","_1"); new dbplyr seems to don't like having no argument 
    filter(!!sym(start_date_col) <= !!sym(end_date_2)
           & !!sym(start_date_2) <= !!sym(end_date_col))
  
  combined_spells = full_join(core_1, core_2, by = c(within, start_date_col, end_date_col)) %>%
    mutate(!!!zz)
  
  ## output ----
  
  required_columns = c(within, start_date_col, end_date_col, cols_to_duplicate, cols_to_proportion)
  combined_spells = combined_spells %>% select(required_columns)
  
  return(combined_spells)
}




