#####################################################################################################
#' Description: Analysis functions for producing Journey Timelines Output
#'
#' Input: As per each function
#'
#' Output: Functions in R environment for use in analysis
#' 
#' Authors: Simon Anastasiadis, Athira Nair, Michael Hackney
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
#' 2019-04-24 AK QA
#' 2019-02-01 AN Reviewed
#' 2019-01-17 SA v0
#####################################################################################################

# pre checks
required_packages = c("TraMineR", "cluster", "Matrix", "tidyverse")
for(package in required_packages)
  if(! (package %in% installed.packages()))
    stop(sprintf("must install %s package",package))

#' Convert data frame to a collection of sequences
#' 
#' Input is a dataframe with two title columns. These columns contain the identities
#' and types of sequences. The names of these columns are given as text strings in
#' the arguments "event_identity" and "sequence_name" respectively.
#' 
#' All columns that are not 'event_identity' or 'sequence_name' are assumed to be 
#' the sequence, in order.
#' 




data_frame_to_sequence_collection = function(df, event_identity, sequence_name){
  # checks
  assert(is.character(event_identity), "event_identity must be of type character")
  assert(is.character(sequence_name), "sequence_name must be of type character")
  assert(is.data.frame(df), "df must be a data.frame")
  assert(event_identity %in% names(df), "event_identity is not a name of data.frame")
  assert(sequence_name %in% names(df), "sequence_name is not a name of data.frame")
  
  # make identity list one & reuse to ensure all sequences have the same order of identities
  identity_list = df %>% select(!!sym(event_identity)) %>% unique()
  identity_list = sort(identity_list[,1])
  # setup
  types_of_sequences = df %>% select(!!sym(sequence_name)) %>% unique()
  types_of_sequences = sort(types_of_sequences[,1])
  df = df %>%
    rename(identity_id = !!sym(event_identity),
           measure_id = !!sym(sequence_name))
  
  # each measure/type-of-sequence becomes a channel
  sequence_collection = list()
  for(ii in 1:length(types_of_sequences)){
    type = types_of_sequences[ii]
    sequences_for_measure = single_measure(df, type, identity_list) %>%
      select(-identity_id)
    sequence_collection[[ii]] = suppressMessages( TraMineR::seqdef(sequences_for_measure) )
  }
  return(list(sequence_collection = sequence_collection,
              identity_list = identity_list))
}

#' Create data.frame for a single measure, where each row corresponds to a single identity.
#' Entries in the df are zeros if the measure did not occur for that identity at that time,
#' and 1 if the measure did occur.
#' Tf the measure did not occur at any point for the identity then a row of zeroes is
#' added for the identity to preserve structure.
#' 
#' Input dataframe has two title columns "identity_id" and "measure_id" containing the
#' identities and types of measures respectively. All other columns are assumed to be 
#' the sequence, in order.
#' 
#' Output dataframe with one title column "identity_id" and the sequence for each identity
#' for the selected measure.
#' 
single_measure = function(df, measure, identity_list){
  # checks
  assert("identity_id" %in% colnames(df), "df must contain column identity_id")
  assert("measure_id" %in% colnames(df), "df must contain column measure_id")
  assert(is.character(measure), "measure must be of type character")
  
  identity_measure <- df %>%
    filter(measure_id == measure) %>%
    complete(identity_id = identity_list) %>%
    select(-measure_id) %>%
    arrange(identity_id)
  measure_not_occurred <- is.na(identity_measure[,-1]) | identity_measure[,-1] == 0
  identity_measure[,-1][measure_not_occurred] <- 0
  identity_measure[,-1][!measure_not_occurred] <- 1
  
  return(identity_measure)
}

#' Compute distances between all pairs of events.
#' 
#' Input, a collection of sequences such as produced by 'data_frame_to_sequence_collection'.
#' 
#' experiments show that the runtime of seqdistmc is non-linear in the number of channels
#' even though the final distance is the sum of all channels. Hence we break processing into
#' batches of channels, with argument: batch_size
#' 
sequence_collection_to_distances = function(sequence_collection, identity_list, batch_size){
  # checks
  assert(nrow(sequence_collection[[1]]) == length(identity_list),
         "different number of sequences and identities")
  assert(batch_size > 1, "batch size of 1 not implemented as multi-channel assumes at least 2 channels")
  
  num_channels = length(sequence_collection)
  index_start = 1
  index_end = min(batch_size, num_channels)
  # Using TraMineR package, calculate the multichannel sequence distance between each pair
  # of identities/events, and store as a matrix. Using "Optimal matching" to determine 
  # sequence distances, with transition rates used for the substitution cost matrix.
  sequence_distance = suppressMessages(
    TraMineR::seqdistmc(channels = sequence_collection[index_start:index_end],
                                          method = "OM", indel = 1)
  )
  
  while(index_end < num_channels){
    index_start = index_end + 1
    index_end = min(index_end + batch_size, num_channels)
    
    # print(sprintf("seq2dist start %i, end %i",index_start, index_end))

    if(index_start == index_end){
      sequence_distance = sequence_distance + 0.5 *
        suppressMessages (
          TraMineR::seqdistmc(sequence_collection[c(index_start, index_end)],
                            method = "OM", indel = 1)
        )
    } else {
      sequence_distance = sequence_distance +
        suppressMessages (
          TraMineR::seqdistmc(channels = sequence_collection[index_start:index_end],
                              method = "OM", indel = 1)
        )
    }
  }
  
  
  # ssm = matrix(cbind(c(0,1),c(1,0)),nrow = 2)
  # TraMineR::seqdist(event_sequences[[1]], method = "OM", indel = 1, sm = ssm)
  
  
  # Force matrix symmmetry to combat round-off error
  sequence_distance = Matrix::forceSymmetric(sequence_distance)
  sequence_distance = matrix(sequence_distance, nrow = length(identity_list), byrow = TRUE)
}

#' Clusters identities into groups based on pairwise distances between them.
#' 
#' Inputs:
#' sequence_distance = a symmetric matrix of pairwise distances.
#' identity_list = the list of identities, matching the row & column order of the distance matrix
#' number of clusters = number of clusters in the output
#' give_plots = indicator for output plotting
#' 
#' Output:
#' A data.frame with two columns identity_list giving the identities
#' and sequence_clusters.clustering giving the grouping.
#'
distances_to_clustered_groups = function(sequence_distance, identity_list, 
                                         number_of_clusters, give_plots = FALSE){
  # checks
  assert(is.matrix(sequence_distance), "distance input must be of type matrix")
  assert(is.logical(give_plots), "plotting output control must be boolean")
  assert(length(identity_list) == nrow(sequence_distance)
         & length(identity_list) == ncol(sequence_distance),
         "different number of identities between identity list and distance matrix")
  
  # Partition using PAM clustering
  sequence_clusters = cluster::pam(x = sequence_distance, k = number_of_clusters, diss = TRUE)
  # combine identity list and cluster
  clustered_identities = data.frame(identity = identity_list, cluster = sequence_clusters$clustering)
  
  
  # plot if required
  if(give_plots){
    # silhouettes
    cluster_silhouettes = cluster::silhouette(sequence_clusters, dmatrix = sequence_distance)
    plot(cluster_silhouettes, col = 1:number_of_clusters,
         main = sprintf("silhouette %d clusters",number_of_clusters))
    # cluster plots
    cluster::clusplot(sequence_distance, clus = sequence_clusters$clustering, diss = TRUE)
  }
  
  return(sequence_clusters)
}

#' Provides batch calculation of distances and clustering. Due to memory and speed constains
#' we borrow some ideas from big data/distributed processing to divide processing up into
#' separate batches that are then aggregated into a final result.
#'
#' This function largely acts as a wrapper for:
#' - data_frame_to_sequence_collection
#' - sequence_collection_to_distances
#' - distances_to_clustered_groups
#' hence it encompases all their inputs, in addition to a number of batches.
#'
batch_processing = function(df, event_identity, sequence_name,
                            channel_batch_size, number_of_clusters,
                            give_plots, number_of_batches){
  # events to cluster
  list_of_identities = df %>% select(!!sym(event_identity)) %>% unique()
  list_of_identities = list_of_identities[,1]
  # dataset it not too big
  assert(length(list_of_identities) / number_of_batches < CRIT_N, "too few batches given size of dataframe")
  
  # setup
  batch_results = list()
  medoid_identities = c()
  
  for(ii in 1:number_of_batches){
    # extract records for this batch
    this_batch = list_of_identities %% number_of_batches == ii-1
    this_identities = list_of_identities[this_batch]
    this_df = filter(df, !!sym(event_identity) %in% this_identities)
    # inform user
    run_time_inform_user(sprintf("-- batch: %d, size: %d", ii, sum(this_batch)))
    
    this_cluster = single_batch(this_df, event_identity, sequence_name,
                                channel_batch_size, number_of_clusters, give_plots = FALSE)
    batch_results[[ii]] = this_cluster
    # prepare medoids for aggregation clustering
    this_medoids = this_cluster %>% select(medoid_id) %>% unique()
    medoid_identities = c(medoid_identities, this_medoids[,1])
  }
  
  # aggregating clustering can be treated as a single batch
  medoid_df = df %>%
    filter(!!sym(event_identity) %in% medoid_identities) %>%
    arrange(!!sym(event_identity))
  medoid_identities = sort(medoid_identities)
  # inform user
  run_time_inform_user(sprintf("-- batch: medoid, size: %d", sum(this_batch)))
  
  medoid_cluster = single_batch(medoid_df, event_identity, sequence_name,
                              channel_batch_size, number_of_clusters, give_plots)
  
  # assemble results
  aggregate_medoids = function(batch_result){
    batch_result %>%
      inner_join(medoid_cluster,by = c("medoid_id" = "id"), suffix = c("","_m")) %>%
      select(event_id = id, medoid = medoid_id_m) %>%
      data.table()
  }
  
  aggregated_medoids = lapply(batch_results,  aggregate_medoids) %>% rbindlist()
}

#' Single batch
#' 
#' Helper function for batch processing. Used to run each batch:
#' - Convert data frame to sequences
#' - Convert sequences to distances
#' - Convert distances to clusters
#'
single_batch = function(this_df, event_identity, sequence_name,
                        channel_batch_size, number_of_clusters, give_plots){
  # convert batch to a collection of sequences for each event
  # (formally this is a multi-channel sequence object, required for computing sequence distances)
  output = data_frame_to_sequence_collection(this_df, event_identity, sequence_name)
  this_sequence_collection = output$sequence_collection
  this_identities = output$identity_list
  # print("sequences complete")
  # compute sequence_distances between all pairs of events in the batch
  this_event_distances = sequence_collection_to_distances(this_sequence_collection,
                                                          this_identities, 
                                                          channel_batch_size)
  # print("dist complete")
  # compute clusters, do not plot results
  this_event_clusters = distances_to_clustered_groups(this_event_distances,
                                                      this_identities,
                                                      number_of_clusters, give_plots)
  # print("clustered")
  # store batch results
  this_cluster = data.frame(id = this_identities, cluster = this_event_clusters$clustering)
  this_medoids = data.frame(cluster = 1:length(this_event_clusters$medoids),
                            medoid_id = this_identities[this_event_clusters$medoids])
  this_cluster = inner_join(this_cluster, this_medoids, by = "cluster")
  # complete
  return(this_cluster)
}

#' Makes representative timelines using optimisation proceedure
#' and distance function.
#' 
make_representative_timeline = function(individual_journeys, num_events, required_columns){
  # checks
  assert(is.data.frame(individual_journeys), "individual_journeys must be a data frame")
  assert(is.numeric(num_events), "num_events must be numeric")
  assert(all(colnames(individual_journeys) %in% required_columns), "timelines lack required columns")
  assert(all(required_columns %in% colnames(individual_journeys)), "timelines contains extra columns")
  
  # start
  initial_vector = initial_timeline(individual_journeys, num_events, required_columns)
  assert(all(colnames(initial_vector) %in% required_columns), "initial timeline lacks required columns")
  assert(all(required_columns %in% colnames(initial_vector)), "initial timeline contains extra columns")
  # run
  final_vector = optimise_distances(initial_vector, individual_journeys)
  # end
  return(final_vector)
}

#' Create intial representative timeline
#' 
#' This becomes the starting point for our optimisation algorithm
#' 
initial_timeline = function(individual_journeys, num_events, required_columns){
  # calculate average response per time period
  avg_events_per_period = lapply(individual_journeys, mean) %>% as.data.frame(optional = TRUE)
  
  # periods with the most events
  avg_outcome = avg_events_per_period %>% 
    gather() %>%
    arrange(desc(value)) %>%
    head(num_events) %>%
    mutate(value = 1) %>%
    spread(key, value)
  
  # ensure all required columns are present
  missing_cols = setdiff(required_columns, names(avg_outcome))
  avg_outcome[missing_cols] = 0
  avg_outcome = avg_outcome[required_columns]
  
  return(avg_outcome)
}

#' Distance function
#' 
#' Computes the distance between a single timeline and a group of
#' timelines.
#' 
#' Note that the distance function is asymmetric, hence case is
#' required when computing the distance between two single
#' timelines.
#'
#'
distance_function = function(single_timeline, group_timelines){
  # checks
  assert(is.data.frame(single_timeline), "timelines must be data frames")
  assert(is.data.frame(group_timelines), "timelines must be data frames")
  assert(ncol(single_timeline) == ncol(group_timelines), "timelines to compare are different lengths")
  assert(all(single_timeline %in% c(0,1)), paste0("timelines must be binary"))
  assert(all(sapply(group_timelines, function(x) all(x %in% c(0,1)))), paste0("timelines must be binary"))
  assert(nrow(single_timeline) == 1, "single timeline is not a single timeline") # ? Some more clarity on the log output required
  
  # convert to matrix
  group_timelines = group_timelines %>% as.matrix() %>% unname()
  
  distance = 0
  
  index_ones = which(single_timeline[1,] == 1)
  index_zero = which(single_timeline[1,] == 0)
  
  # iterate throughcolumns of matrix = each time point
  for(i in 1:ncol(group_timelines)){
    if(single_timeline[1,i] == 1){
      # true value is one, zeros are mismatch, ones are index to match
      num_mismatch = sum(group_timelines[,i] == 0)
      index_to_match = index_ones
    } else {
      # true value is zero, ones are mismatch, zeros are index to match
      num_mismatch = sum(group_timelines[,i] == 1)
      index_to_match = index_zero
    }
    distance = distance + num_mismatch * min(abs(i - index_to_match))
  }
  return(distance)
}
#' Optimisation proceedure for representative timeline
#' 
#' Swaps values and check for improvement in distance function.
#' Greedily keep the best improvement.
#' 
optimise_distances = function(initial_vector, group_timelines, debug = FALSE){
  # setup
  current_vector = initial_vector
  current_distance = distance_function(current_vector, group_timelines)
  
  # begin outer loop, max 100 iterations
  for(jj in 1:100){
    # current state to improve on
    new_distance = current_distance
    new_vector = current_vector
    
    # begin inner loop, find the single swap that most reduces the distance function
    for(ii in 2:length(current_vector)){
      # swap if different
      if(current_vector[ii-1] != current_vector[ii]){
        # outcome after swap
        swap_vector = replace(current_vector, c(ii-1, ii), current_vector[c(ii, ii-1)])
        swap_distance = distance_function(swap_vector, group_timelines)
        if(debug) print(swap_distance)
        # record if better
        if(swap_distance < new_distance){
          new_distance = swap_distance
          new_vector = swap_vector
        }
      }
    }
    # break if no single swap produced an improvement
    if(new_distance == current_distance) break
    # else update current vector & distance
    current_distance = new_distance
    current_vector = new_vector
    if(debug) print("improvement")
    if(debug) print(new_distance)
    if(debug) print(new_vector)
    # and repeat
  }
  
  return(current_vector)
}



