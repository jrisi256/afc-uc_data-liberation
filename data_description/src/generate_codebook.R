library(here)
library(dplyr)
library(readr)
library(purrr)
library(tidyr)
library(stringr)
library(flextable)
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
  )

afc_uc_table_list <- append(afc_uc_table_list, list(sir_event_table))
names(afc_uc_table_list)[20] <- "UAC_SIR_EVENT_DATA_TABLE"

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
  function(df, cut = "max", nmax = 5, nmin = 5) {
    df <-
      df %>%
      # Convert data from wide to long
      pivot_longer(
        cols = everything(),
        names_to = "variable",
        values_to = "value",
        values_transform = as.character
      ) %>%
      # Count the number of times each value of a variable appears
      count(variable, value, name = "nr") %>%
      group_by(variable) %>%
      mutate(
        prop = nr / sum(nr),
        nr_unique = length(unique(value))
      )

    # Return largest, smallest, or both largest and smallest values
    if (cut == "max") {
      df <- df %>% slice_max(nr, n = nmax, with_ties = F)
    } else if (cut == "min") {
      df <- df %>% slice_min(nr, n = nmin, with_ties = F)
    } else if (cut == "min_max") {
      df_max <- df %>% slice_max(nr, n = nmax, with_ties = F)
      df_min <- df %>% slice_min(nr, n = nmin, with_ties = F)
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
            !(matches("_ID|CASE.*NUMBER|TIME|DATE_|_DATE$"))
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
      # Convert data from wide to long
      pivot_longer(
        cols = everything(),
        names_to = "variable",
        values_to = "value",
        values_transform = as.character
      ) %>%
      group_by(variable) %>%
      # Count number of unique values for the ID and number missing
      summarise(
        nr_rows = n(),
        nr_unique = length(unique(value)),
        nr_missing = sum(is.na(value)),
        prop_missing = round(sum(is.na(value)) / n(), digits = 3)
      )
  }

# Summarize ID columns
df_id <-
  map(
    afc_uc_table_list,
    function(df) {
      df <- df %>% select((matches("_ID|CASE.*NUMBER")))

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
      # Convert data from wide to long
      pivot_longer(
        cols = everything(), names_to = "variable", values_to = "value"
      ) %>%
      group_by(variable) %>%
      # Describe distribution of data
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
        prop_missing = round(sum(is.na(value)) / n(), digits = 3)
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

#################################################################
##     Create descriptive tables for dates and date times.     ##
#################################################################
standardize_long_dates <- function(df, table_name) {
  ######################## Combine these columns into one column.
  if (all(c("DATE_NOTIFIED", "TIME_NOTIFIED") %in% colnames(df))) {
    df <-
      df %>%
      mutate(
        DATETIME_NOTIFIED =
          ymd_hms(paste0(as_date(mdy_hm(DATE_NOTIFIED)), " ", TIME_NOTIFIED))
      ) %>%
      select(-DATE_NOTIFIED, -TIME_NOTIFIED)
  }

  if (all(c("DATE_EVENT", "TIME_EVENT") %in% colnames(df))) {
    df <-
      df %>%
      mutate(
        DATETIME_EVENT =
          ymd_hms(paste0(as_date(mdy_hm(DATE_EVENT)), " ", TIME_EVENT))
      ) %>%
      select(-DATE_EVENT, -TIME_EVENT)
  }

  if (all(c("DATE_LAW_REPORT", "TIME_LAW_REPORT") %in% colnames(df))) {
    df <-
      df %>%
      mutate(
        DATETIME_LAW_REPORT =
          ymd_hms(paste0(as_date(mdy_hm(DATE_LAW_REPORT)), " ", TIME_LAW_REPORT))
      ) %>%
      select(-DATE_LAW_REPORT, -TIME_LAW_REPORT)
  }

  if (all(c("DATE_OF_EVENT", "TIME_OF_EVENT") %in% colnames(df))) {
    df <-
      df %>%
      mutate(
        DATETIME_OF_EVENT =
          ymd_hms(paste0(as_date(mdy_hm(DATE_OF_EVENT)), " ", TIME_OF_EVENT))
      ) %>%
      select(-DATE_OF_EVENT, -TIME_OF_EVENT)
  }

  if (all(c("DATE_STATE_REPORT", "TIME_STATE_REPORT") %in% colnames(df))) {
    df <-
      df %>%
      mutate(
        DATETIME_STATE_REPORT =
          ymd_hms(paste0(as_date(mdy_hm(DATE_STATE_REPORT)), " ", TIME_STATE_REPORT))
      ) %>%
      select(-DATE_STATE_REPORT, -TIME_STATE_REPORT)
  }

  df <-
    df %>%
    ######################## Make the date and time columns long.
    pivot_longer(
      cols = everything(),
      names_to = "variable",
      values_to = "value",
      values_transform = as.character
    ) %>%
    # The specific variable will dictate how to parse the date/date time.
    mutate(
      date =
        case_when(
          is.na(value) ~ NA_Date_,
          str_detect(variable, "DATE_CONTACT|TIME") ~ ymd_hms(value),
          str_detect(variable, "DATE_CREATED") & table_name == "PROGRAM_LEVEL_EVENTS_INFO_DATA_TABLE" ~ ymd_hms(value),
          str_detect(variable, "DATE") ~ parse_date_time(value, "%m/%d/%y %H:%M", exact = T)
        ),
      # Was the date missing or was it not able to be parsed?
      date_status =
        case_when(
          is.na(value) ~ NA_character_,
          !is.na(value) & is.na(date) ~ "Parse failed.",
          !is.na(date) ~ "Parse succeeded"
        )
    )
}

df_dates_std_long <-
  pmap(
    list(afc_uc_table_list, names(afc_uc_table_list)),
    function(df, table_name) {
      df <-
        df %>%
        select(matches("TIME|DATE_|_DATE$"))

      if (ncol(df) != 0) {
        df <- standardize_long_dates(df, table_name)
      } else {
        return(df %>% filter(F))
      }
    }
  )

# Describe date and date time variables as if they were numbers.
describe_columns_num <- function(df) {
  df %>%
    group_by(variable) %>%
    summarise(
      min = min(date, na.rm = T),
      p25 = quantile(date, probs = 0.25, na.rm = T, type = 1)[["25%"]],
      median = median(date, na.rm = T),
      p75 = quantile(date, probs = 0.75, na.rm = T, type = 1)[["75%"]],
      max = max(date, na.rm = T),
      nr_missing = sum(is.na(value)),
      nr_failed_to_parse = sum(date_status == "Parse failed.", na.rm = T),
      prop_missing_or_fail = round(sum(is.na(date)) / n(), digits = 3)
    )
}

df_dates_num <-
  map(
    df_dates_std_long,
    function(df) {
      if (ncol(df) != 0) {
        df <- describe_columns_num(df)
      } else {
        return(df %>% filter(F))
      }
    }
  )

describe_columns_category <- function(df) {
  df %>%
    # Describe the days, month, year, days of week, and hour of day.
    mutate(
      day = mday(date),
      month = month(date),
      year = year(date),
      day_of_week = wday(date),
      hour = hour(round_date(date, unit = "hour"))
    ) %>%
    select(-date, -date_status, -value) %>%
    # Convert data from wide to long.
    pivot_longer(
      cols = day:hour,
      names_to = "unit_of_time",
      values_to = "value"
    ) %>%
    # Count number of times an observation happens during each period of time.
    count(variable, unit_of_time, value, name = "nr") %>%
    group_by(variable, unit_of_time) %>%
    mutate(prop = nr / sum(nr)) %>%
    ungroup()
}

df_dates_category <-
  map(
    df_dates_std_long,
    function(df) {
      if (ncol(df) != 0) {
        df <- describe_columns_category(df)
      } else {
        return(df %>% filter(F))
      }
    }
  )

#################################################################
##             Make sure every column was captured             ##
#################################################################
check <-
  pmap(
    list(afc_uc_table_list, df_c_l, df_dates_num, df_id, df_n),
    function(df, cl, date, id, n) {
      nr_vars <- length(colnames(df))
      nr_analyzed <-
        length(unique(cl$variable)) +
        length(unique(date$variable)) +
        length(unique(id$variable)) +
        length(unique(n$variable))
      return(list(nr_vars = nr_vars, nr_analyzed = nr_analyzed))
    }
  )

#################################################################
##                      Write out results                      ##
#################################################################
set_flextable_defaults(
  digits = 3, font.size = 6, theme_fun = theme_vanilla, na_str = "<na>"
)

pmap(
  list(df_c_l, df_id, df_n, df_dates_num, df_dates_category, names(df_c_l)),
  function(c_l, id, n, date_num, date_cat, tbl_name) {
    # If any of the data frames are empty, say so.
    if(ncol(c_l) == 0) {
      c_l <- tibble(col = "No categorical variables.")
    }
    
    if(ncol(id) == 0) {
      id <- tibble(col = "No ID variables.")
    }
    
    if(ncol(n) == 0) {
      n <- tibble(col = "No numerical variables.")
    }
    
    if(ncol(date_num) == 0) {
      date_num <- tibble(col = "No date variables.")
      date_cat <- tibble(col = "No date variables.")
    }
    
    # Convert each data frame into flextable objects.
    flextables <-
      map(
        list(
          "Categorical variables" = c_l, 
          "ID variables" = id, 
          "Numerical variables" = n, 
          "Date variables (numeric)" = date_num, 
          "Date variables (categorical)" = date_cat
        ),
        function(df) {
          df %>%
            qflextable() %>%
            set_table_properties(layout = "autofit", width = 1) %>%
            colformat_double()
        }
      )
    
    # Save all the flextables.
    save_as_docx(
      values = flextables,
      path = here("data_description", "output", paste0(tbl_name, ".docx"))
    )
  }
)
