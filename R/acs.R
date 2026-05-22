
source("R/util.R", local = (util <- new.env()))

# input paths
ipath <- list(
  acs5_api_ = "https://api.census.gov/data/{year}/acs/acs5?get=group({tabid})&ucgid={ucgid}&key={key}"
)

# output paths
opath <- list(
  acs5_table_ = "data/raw/acs5/{year}/{tabid}.pq"
)

#' Retrieve ACS-5 table from Census Data API
#' US, state and county tables are downloaded in separate calls and combined into single dataframe
#' Dataframes are cached to local parquet file for subsequent access
get_raw_table <- function(year, tabid) {

  cache_path <- stringr::str_glue(opath$acs5_table_)
  if (file.exists(cache_path)) {
    logger::log_info("Loading ACS-5 {year} table {tabid} from cache {cache_path}")
    return(arrow::read_parquet(cache_path))
  }

  key <- Sys.getenv("CENSUS_API_KEY")
  if (key == "") {
    logger::log_error("CENSUS_API_KEY environmental variable is not set.")
    stop()
  }

  # UCGID parameter specifications for US, all states and all counties
  df <- list(
    US = "0100000US",
    state = "pseudo(0100000US$0400000)",
    county = "pseudo(0100000US$0500000)"
  ) |>
    # perform API call for each UCGID and then combine results into single dataframe
    purrr::imap(\(ucgid, geo_type) {
      url <- stringr::str_glue(ipath$acs5_api_)
      logger::log_info("Requesting ", geo_type, " data from API endpoint ", url)
      resp_json <- url |>
        httr2::request() |>
        httr2::req_perform() |>
        httr2::resp_body_json()

      # response is list of lists
      # first element is list of column names
      col_names <- unlist(resp_json[[1]])
      # numeric estimate and MOE columns look like B15001_001E or S0101_C02_031M
      num_cols <- grepv(paste0("^", tabid, "_.*[EM]$"), col_names)
      # all remaining elements are lists of values for every row
      # take all non-header rows, give column names to their elements and combine into a dataframe
      resp_json[-1] |>
        purrr::map(\(row) {
          # empty cells are NULL in json list
          # set them to NA, otherwise all-null columns are dropped when bind_rows()
          row[purrr::map_lgl(row, is.null)] <- NA_character_
          stats::setNames(row, col_names)
        }) |>
        dplyr::bind_rows() |>
        dplyr::relocate(GEO_ID, NAME, ucgid) |>
        # convert character values to double for numeric columns
        dplyr::mutate(dplyr::across(all_of(num_cols), readr::parse_double))
    }) |>
    # combine US, state and column frames
    dplyr::bind_rows()
  
  logger::log_info("Saving ACS-5 {year} table {tabid} to cache {cache_path}")
  arrow::write_parquet(df, util$mkdir(cache_path))
  df
}