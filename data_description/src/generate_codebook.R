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

# Change some of the column types from numeric to character.
afc_uc_table_list$UAC_SIR_CATEGORY_DATA_TABLE <-
  afc_uc_table_list$UAC_SIR_CATEGORY_DATA_TABLE %>%
  mutate(DESCRIPTION_ORDER = as.character(DESCRIPTION_ORDER))

afc_uc_table_list$UAC_SIR_FOLLOWUP_RPT_DATA_TABLE <-
  afc_uc_table_list$UAC_SIR_FOLLOWUP_RPT_DATA_TABLE %>%
  mutate(
    CARE_PROVIDER_CITY = as.character(CARE_PROVIDER_CITY),
    CARE_PROVIDER_NAME = as.character(CARE_PROVIDER_NAME),
    CARE_PROVIDER_STATE = as.character(CARE_PROVIDER_STATE)
  )

####################################################################
##  Create descriptive tables for character and logical columns.  ##
####################################################################
describe_columns_c_l <-
  function(df, order = "large", cut = "max", nmax = 5, nmin = 5, cum_sum = NA) {
    df <-
      df %>%
      pivot_longer(
        cols = everything(),
        names_to = "variable",
        values_to = "value",
        values_transform = as.character
      ) %>%
      count(variable, value, name = "nr") %>%
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
      df <-
        df %>%
        slice_max(nr, n = nmax, with_ties = F)
    } else if (cut == "min") {
      df <- df %>% slice_min(nr, n = nmin)
    } else if (cut == "min_max") {
      df_max <- df %>% slice_max(nr, n = nmax)
      df_min <- df %>% slice_min(nr, n = nmin)
      df <- bind_rows(df_max, df_min) %>% arrange(variable)
    }

    df <- df %>% ungroup()
  }

# Summarize character and logical columns
df_c_l <-
  map(
    afc_uc_table_list,
    function(df) {
      df <-
        df %>%
        select(
          (where(is.character) |
             where(is.logical) |
             where(~ all(.x == 0 | .x == 1 | .x == 2, na.rm = T))) &
            !(matches("_ID|CASE_NUMBER"))
        )

      if (ncol(df) != 0) {
        df <- describe_columns_c_l(df)
      } else {
        return(df %>% filter(F))
      }
    }
  )

##################################################################
##           Create descriptive tables for ID columns           ##
##################################################################
describe_columns_id <-
  function(df) {
    df <-
      df %>%
      pivot_longer(
        cols = everything(), names_to = "variable", values_to = "value"
      ) %>%
      group_by(variable) %>%
      summarise(
        nr_rows = n(),
        nr_unique = length(unique(value)),
        nr_missing = sum(is.na(value)),
        prcnt_missing = round(sum(is.na(value)) / n(), digits = 3)
      )
  }

# Summarize ID columns
df_id <-
  map(
    afc_uc_table_list,
    function(df) {
      df <- df %>% select((matches("_ID|CASE_NUMBER")))
      
      if (ncol(df) != 0) {
        df <- describe_columns_id(df)
      } else {
        return(df %>% filter(F))
      }
    }
  )

#################################################################
##        Create descriptive tables for numeric columns        ##
#################################################################
describe_columns_n <-
  function(df) {
    df <-
      df %>%
      pivot_longer(
        cols = everything(), names_to = "variable", values_to = "value"
      ) %>%
      group_by(variable) %>%
      summarise(
        mean = round(mean(value, na.rm = T), digits = 3),
        sd = round(sd(value, na.rm = T), digits = 3),
        min = round(min(value, na.rm = T), digits = 3),
        p25 =
          round(quantile(value, probs = 0.25, na.rm = T)[["25%"]], digits = 3),
        median = round(median(value, na.rm = T), digits = 3),
        p75 =
          round(quantile(value, probs = 0.75, na.rm = T)[["75%"]], digits = 3),
        max = round(max(value, na.rm = T), digits = 3),
        iqr = round(IQR(value, na.rm = T), digits = 3),
        mad = round(mad(value, na.rm = T), digits = 3),
        nr_missing = sum(is.na(value)),
        prcnt_missing = round(sum(is.na(value)) / n(), digits = 3)
      ) %>%
      ungroup()
  }

# Summarize character and logical columns
df_n <-
  map(
    afc_uc_table_list,
    function(df) {
      df <-
        df %>%
        select(
          (where(is.numeric) &
             where(~ any(.x != 0 & .x != 1 & .x != 2, na.rm = T))) &
            !(matches("_ID|CASE_NUMBER"))
        )
      
      if (ncol(df) != 0) {
        df <- describe_columns_n(df)
      } else {
        return(df %>% filter(F))
      }
    }
  )












# Write out results
pwalk(
  list(df_c_l, names(df_c_l)),
  function(df, file_name) {
    write_csv(df, here(paste0(file_name, "_codebook.csv")))
  }
)

# date and time columns
# hms::is_hms(afc_uc_table_list[[6]]$TIME_NOTIFIED)
# map(afc_uc_table_list, spec)
# return empty data frames with column names?
