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
names(afc_uc_table_list)[20] <- "UAC_SIR_EVENT_DATA_TABLE"

#################################################################
##                   Analyze overlap in IDs.                   ##
#################################################################
afc_uc_table_list_ids <-
  map(
    afc_uc_table_list,
    function(df) {
      df <- df %>% select((matches("_ID|CASE.*NUMBER")))
    }
  )

test <-
  pmap_dfr(
    list(afc_uc_table_list_ids, names(afc_uc_table_list_ids)),
    function(df, table_name) {
      df %>%
        slice(1) %>%
        mutate(across(everything(), function(col) {col = T})) %>%
        mutate(table = table_name) %>%
        relocate(table)
    }
  ) %>%
  mutate(across(where(is.logical), function(col) {if_else(is.na(col), F, col)}))

test2 <-
  map(
    select(test, -table),
    function(col) {
      test %>% filter({{ col }}) %>% pull(table)
    }
  )

keep <- unlist(map(test2, function(x) length(x)))
keep <- keep[keep > 1]
keep <- as.list(names(keep))

test3 <-
  pmap(
    list(test2[unlist(keep)], keep),
    function(table_names, col_name) {
      pmap(
        list(afc_uc_table_list_ids[table_names], as.list(table_names)),
        function(df, table_name) {
          df %>%
            select(all_of(col_name)) %>%
            mutate(source = table_name, in_table = T) %>%
            distinct(.data[[col_name]], .keep_all = T) %>%
            pivot_wider(names_from = source, values_from = in_table)
        }
      ) %>%
        reduce(function(df1, df2) {
          full_join(df1, df2, by = col_name)
        }) %>%
        mutate(across(everything(), ~ if_else(is.na(.x), F, .x)))
    }
  )

pmap(
  list(test3[8], names(test3[8])),
  function(df, key_name) {
    columns <- colnames(df)[2:ncol(df)]
    
    upset(
      df,
      columns,
      name = "Tables",
      width_ratio = 0.1,
      wrap = T,
      set_sizes=(
        upset_set_size() +
          theme(
            axis.text.x = element_text(angle = 90, size = 7),
            axis.title.x = element_text(size = 6)
          )
      )
    ) +
      ggtitle(key_name)
  }
)
