library(here)
library(dplyr)
library(readr)
library(purrr)
library(tidyr)
library(stringr)
library(lubridate)

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

##################################################################
##                  Create descriptive tables.                  ##
##################################################################

# Summarize character and logical columns
df_c_l <-
  afc_uc_table_list[[3]] %>%
  select((where(is.character) | where(is.logical)) & !(matches("_ID"))) %>%
  describe_columns_c_1()

# df_n <- df %>% select(where(is.numeric))
# date and time columns

describe_columns_c_l <-
  function(df, order = "large", cut = "max", nmax = 5, nmin = 5, cum_sum = NA) {
    df <-
      df %>%
      pivot_longer(
        cols = everything(),
        names_to = "variable",
        values_to = "value"
      ) %>%
      count(variable, value, name = nr) %>%
      group_by(variable) %>%
      mutate(
        percent = nr / sum(nr),
        nr_unique = length(unique(value))
      )

    if (order == "large") {
      df <- df %>% arrange(desc(nr), .by_group = T)
    } else if (order == "small") {
      df <- df %>% arrange(nr, .by_group = T)
    }

    df <- df %>% mutate(cumulative_percent = cumsum(percent))

    if (!is.na(cum_sum)) {
      df <- df %>% filter(cumlative_percent <= cum_sum)
    } else if (cut == "max") {
      df <- df %>% slice_max(nr, n = nmax)
    } else if (cut == "min") {
      df <- df %>% slice_min(nr, n = nmin)
    } else if (cut == "min_max") {
      df_max <- df %>% slice_max(nr, n = nmax)
      df_min <- df %>% slice_min(nr, n = nmin)
      df <- bind_rows(df_max, df_min) %>% arrange(variable)
    }

    df <- df %>% ungroup()
  }

hms::is_hms(afc_uc_table_list[[6]]$TIME_NOTIFIED)
map(afc_uc_table_list, spec)
