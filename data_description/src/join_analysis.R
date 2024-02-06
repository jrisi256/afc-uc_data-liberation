library(here)
library(dplyr)
library(readr)
library(purrr)

##################################################################
##                         Read in data                         ##
##################################################################

# import sas var1 var2 var3 using "data.sas7bdat", rowrange(1:1000)
# import sas using "data.sas7bdat", rowrange(1:1000)

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

