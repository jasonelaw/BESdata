#'Submits a query to JANUS database
#'
#'Submits a query to JANUS database view JANUS_ELEMENT using
#'arbitrary tag=value query parameters
#'
#'The fields available to query from are:sample_code, janus_project_name,
#'location_id, location_code, sample_point, location_description,
#'element_sample_name, sample_begin_time, sample_end_time, sample_type,
#'matrix, method_code, element_analysis_name, analyte_id, element_analyte_name,
#'anus_analyte_name, result_spec, result_op, numeric_result, analyte_units,
#'combined_result, text_result_mdl, text_result_mrl, lab, project_id, depth,
#'depth_units, hansen_id, export_to_wqdb, comments, qualifiers,
#'analyte_result_id, janus_sample_id, collected_by, department, general_method,
#'Client, update_date, and Wrk.
#'
#'The user running R must have permission to access the database view and
#'must have a ODBC Data Source set up for the view on the current computer.
#'The reporting server version of the table is currently BESREPORTS.WATERSHED.JANUS_ELEMENT.
#'In addition, different ODBC connections must be set up for 32 and 64 bit
#'versions of R. The \code{RODBC::odbcDataSources()} function can be used to
#'check the currently available data sources from within R.
#'
#'@param ... query arguments in the form 'field = value'.
#'@param start start date of data range
#'@param end end date of data range
#'@param dsn the dsn for the JANUS database on the current system
#'@param date.field the field used for the start and end query arguments.
#'@return a dataframe of results
#'@export
#'@author Jason Law jlaw@@portlandoregon.gov
read.janus <- function (..., start = NULL, end = NULL, dsn = 'JANUS_REP_64',  date.field = 'sample_end_time'){
  con <- dbConnect(dsn)
  on.exit(dbDisconnect(con))
  table  <- 'JANUS_ELEMENT'#"V_RPT_JANUS_ELEMENT"
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = date.field)
  query <- constructQuery(table, where, unrestricted = F)
  ret <- dbGetQuery(con, query)

  kSort  <- c('location_code', 'method_code', 'janus_analyte_name', 'sample_end_time')
  kDates <- c('sample_begin_time', 'sample_end_time', 'update_date')
  ret <- formatDataFrame(ret, sort = kSort, date = kDates, parseDate = parseLocalTime)
  ret$combined_result <- ifelse(is.na(ret$combined_result), ret$numeric_result, ret$combined_result)
  ret$text_result     <- ret$combined_result
  ret$nd <- stringi::stri_extract_first_regex(ret$combined_result, '^[<>]')
  ret$numeric_result <- stringi::stri_replace_all_regex(ret$combined_result, '^[<>=]{1,2}|^EST', '')
  ret$numeric_result <- suppressWarnings(as.numeric(ret$numeric_result))
  return(ret)
}

i <- c("janus_project_name",
  "location_code", "sample_point", "location_description", "hansen_id",
  "sample_code", "element_sample_name", "sample_begin_time", "sample_end_time", "collected_by", "sample_type", "matrix","depth", "depth_units",
  "element_analysis_name", "method_code", "department", "general_method",
  "janus_analyte_name", "element_analyte_name", "result_spec", "result_op", "numeric_result", "analyte_units", "combined_result", "text_result_mdl", "text_result_mrl","qualifiers",
  "lab",   "export_to_wqdb","comments", "Client", "update_date", "Wrk",
  "row_id", "project_id", "location_id", "analyte_result_id", "janus_sample_id","analyte_id")
