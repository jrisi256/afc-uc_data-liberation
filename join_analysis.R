library(here)
library(dplyr)
library(readr)
library(purrr)

##################################################################
##                         Read in data                         ##
##################################################################
input_data_dir <- here("data_description", "input")
table_names <- str_replace(list.files(input_data_dir), ".csv", "")
table_paths <- list.files(input_data_dir, full.names = T)

# Readr's parser incorrectly guesses some of the column types. Must do manually.
table_names <- table_names[-c(15)]
table_paths <- table_paths[-c(15)]

# Increase the number of lines the parser reads to guess the column's data type.
# UAC_SIR_FOLLOWUP_RPT_DATA_TABLE is the problematic table in this regard.
afc_uc_table_list <- map(table_paths, read_csv, guess_max = 2000)
names(afc_uc_table_list) <- table_names

# Add manual table
sir_event_table <-
  read_csv(
    file.path(input_data_dir, "UAC_SIR_EVENT_DATA_TABLE.csv"),
    col_types = list(TIME_OF_EVENT = "c")
  ) %>%
  mutate(TIME_OF_EVENT_parse = parse_time(TIME_OF_EVENT, format = ""))

afc_uc_table_list <- append(afc_uc_table_list, list(sir_event_table))
names(afc_uc_table_list)[20] <- "UAC_SIR_FOLLOWUP_RPT_DATA_TABLE"

#################################################################
##                   Analyze overlap in IDs.                   ##
#################################################################
afc_uc_table_list_ids <-
  map(
    afc_uc_table_list,
    function(df) {
      df <- df %>% select((matches("_ID|CASE_NUMBER")))
    }
  )

fileConn <- file(here("output.txt"))
writeLines(c("Hello","World"), fileConn)
close(fileConn)

a <-
  afc_uc_table_list_ids$FOLLOW_UP_CONTACT_DATA_TABLE %>%
  select(EVENT_ID) %>%
  mutate(source = "FOLLOW_UP_CONTACT_DATA_TABLE")

b <-
  afc_uc_table_list_ids$ORR_NOTIFICATION_DATA_TABLE %>%
  select(EVENT_ID) %>%
  mutate(source = "ORR_NOTIFICATION_DATA_TABLE ")

join_a_b <-
  full_join(
    a, b, by = "EVENT_ID", relationship = "many-to-many", na_matches = "never"
  ) %>%
  mutate(id = "EVENT_ID")

# merge 1:m a using b on EVENT_ID
# rename EVENT_ID id

join_analysis <-
  join_a_b %>%
  mutate(
    both_match = if_else(!is.na(source.x) & !is.na(source.y), 1, 0),
    t1_match = if_else(!is.na(source.x) & is.na(source.y), 1, 0),
    t2_match = if_else(is.na(source.x) & !is.na(source.y), 1, 0),
    t1_missing = if_else(!is.na(source.x) & is.na(EVENT_ID), 1, 0),
    t2_missing = if_else(!is.na(source.y) & is.na(EVENT_ID), 1, 0)
  ) %>%
  summarise(
    id = unique(id),
    nr_unique = length(unique(EVENT_ID)),
    t1 = unique(na.omit(source.x)),
    t2 = unique(na.omit(source.y)),
    nr_obs = n(),
    both_match = sum(both_match),
    t1_only = sum(t1_match),
    t2_only = sum(t2_match),
    t1_missing = sum(t1_missing),
    t2_missing = sum(t2_missing)
  )

# merge m:1 join_a_b using b on EVENT_ID, nomatch
# 
# gen both_match = (source_x != . & source_y != .)
# gen t1_match = (source_x != . & source_y == .)
# gen t2_match = (source_x == . & source_y != .)
# gen t1_missing = (source_x != . & EVENT_ID == .)
# gen t2_missing = (source_y != . & EVENT_ID == .)
# 
# drop if missing(EVENT_ID)
# 
# g id = EVENT_ID
# g nr_unique = _N
# g t1 = unique(source_x)
# g t2 = unique(source_y)
# g nr_obs = _N
# g both_match = sum(both_match)
# g t1_only = sum(t1_match)
# g t2_only = sum(t2_match)
# g t1_missing = sum(t1_missing)
# g t2_missing = sum(t2_missing)
# 
# summarize id, nr_unique, t1, t2, nr_obs, both_match, t1_only, t2_only, t1_missing, t2_missing

# This code first merges the two datasets join_a_b and b on the EVENT_ID variable using a one-to-many merge. The m:1 option specifies that there can be multiple records in join_a_b for each record in b. The nomatch option specifies that records with missing values in the merge variable should be kept in the merged dataset and assigned a missing value for the merge variable.
# 
# The code then generates the following variables:
#   
#   both_match: A binary variable that indicates whether there is a match for both source.x and source.y for a given EVENT_ID.
# t1_match: A binary variable that indicates whether there is a match for source.x but not source.y for a given EVENT_ID.
# t2_match: A binary variable that indicates whether there is a match for source.y but not source.x for a given EVENT_ID.
# t1_missing: A binary variable that indicates whether source.x is not missing but EVENT_ID is missing for a given EVENT_ID.
# t2_missing: A binary variable that indicates whether source.y is not missing but EVENT_ID is missing for a given EVENT_ID.
# 
# The code then drops records with missing values in the EVENT_ID variable.
# 
# Finally, the code summarizes the data by calculating the following statistics:
#   
#   id: The unique values of the id variable.
# nr_unique: The number of unique values of the EVENT_ID variable.
# t1: The unique values of the source.x variable.
# t2: The unique values of the source.y variable.
# nr_obs: The number of observations in the dataset.
# both_match: The sum of the both_match variable.
# t1_only: The sum of the t1_match variable.
# t2_only: The sum of the t2_match variable.
# t1_missing: The sum of the t1_missing variable.
# t2_missing: The sum of the t2_missing variable.