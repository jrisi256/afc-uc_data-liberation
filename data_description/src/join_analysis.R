library(here)
library(dplyr)
library(readr)
library(purrr)
library(tidyr)
library(stringr)
library(ggplot2)
library(ComplexUpset)

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

# Which tables have which IDs?
tables_have_which_keys <-
  pmap_dfr(
    list(afc_uc_table_list_ids, names(afc_uc_table_list_ids)),
    function(df, table_name) {
      df %>%
        # Only need 1 row
        slice(1) %>%
        # For each ID column, change value to True
        mutate(across(everything(), function(col) {col = T})) %>%
        # Add the table name as a column
        mutate(table = table_name) %>%
        relocate(table)
    }
  ) %>%
  # For each ID column, if it is missing change it to false
  mutate(across(where(is.logical), function(col) {if_else(is.na(col), F, col)}))

# Which keys are in which tables?
keys_in_which_table <-
  map(
    # For each ID
    select(tables_have_which_keys, -table),
    # Count the number of tables it appears in
    function(df, col) {df %>% filter({{ col }}) %>% pull(table)},
    df = tables_have_which_keys
  )

# Keep only those IDs which appear in more than one table.
keep_ids <- unlist(map(keys_in_which_table, function(x) length(x)))
keep_ids <- keep_ids[keep_ids > 1]
keep_ids <- as.list(names(keep_ids))

distinct_ids_in_each_table <-
  # For each ID variable that is in more than one table
  pmap(
    list(
      keys_in_which_table[unlist(keep_ids)],
      keep_ids,
      list(afc_uc_table_list_ids)
    ),
    # For each table (that has the current ID variable)
    function(table_names, col_name, raw_tables) {
      pmap(
        list(
          raw_tables[table_names],
          as.list(table_names),
          col_name
        ),
        function(df, table_name, id_col) {
          # Keep all distinct values of the ID variable and make table long
          df %>%
            select(all_of(id_col)) %>%
            mutate(source = table_name, in_table = T) %>%
            distinct(.data[[id_col]], .keep_all = T) %>%
            pivot_wider(names_from = source, values_from = in_table)
        }
      ) %>%
        # Join together all tables which contain same ID variable to see which
        # values of that ID appear in which table.
        reduce(function(df1, df2) {full_join(df1, df2, by = col_name)}) %>%
        mutate(across(everything(), ~ if_else(is.na(.x), F, .x)))
    }
  )

upset_plots <-
  pmap(
    list(distinct_ids_in_each_table, names(distinct_ids_in_each_table)),
    function(df, id_name) {
      # Drop the distinct column IDs
      columns <- colnames(df)[2:ncol(df)]
      
      # Create an upset plot
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
        ggtitle(id_name)
    }
)

pmap(
  list(upset_plots, names(upset_plots)),
  function(plot, file_name) {
    ggsave(
      filename = paste0(file_name, ".png"),
      plot = plot,
      device = "png",
      path = here("data_description", "output"),
      width = 18,
      height = 8,
      units = "in"
      )
  }
)
