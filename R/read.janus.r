#'Submits a query to JANUS database
#'
#'Submits a query to JANUS database view JANUS_ELEMENT using
#'arbitrary tag=value query parameters.
#'
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
#'@param as.text logical, return a text (TRUE) or numeric (FALSE) nondetect indicator in column `nd`.
#'@return a dataframe of results
#'@export
#'@author Jason Law jlaw@@portlandoregon.gov
#'@examples
#'\dontrun{
#'#List the available fields
#'library(RODBC)
#'con <- BESdata:::dbConnect("JANUS")
#'sqlColumns(con, 'JANUS_ELEMENT')
#'#Get the last 2 weeks of data
#'read.janus(start = Sys.Date() - 14)
#'}
read.janus <- function (..., start = NULL, end = NULL, dsn = NULL,  date.field = 'sample_end_time', as.text = T, simple = F){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  table  <- 'JANUS_ELEMENT'#"V_RPT_JANUS_ELEMENT"
  if(simple){
    fields <- c("project_name", "location_code", "sample_code", "sample_name", "sample_type", "matrix",
                "sample_begin_time", "sample_end_time", "janus_analyte_name", "combined_result", "analyte_units", "method_code")
  } else {
    fields <- "*"
  }
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = date.field)
  query <- constructQuery(table, where, unrestricted = F, fields = fields)
  ret <- dbGetQuery(con, query)

  kSort  <- c('location_code', 'method_code', 'janus_analyte_name', 'sample_end_time')
  kDates <- c('sample_begin_time', 'sample_end_time')
  ret <- formatDataFrame(ret, sort = kSort, date = kDates, parseDate = parseLocalTime)
  ret$nd             <- parse.nd(ret$combined_result, as.text = as.text)
  ret$numeric_result <- parse.result(ret$combined_result)
  return(ret)
}

parse.nd <- function(x, as.text = T){
  lt <- stri_detect_fixed(x, '<')
  gt <- stri_detect_fixed(x, '>')
  nd <- ifelse(lt, -1, ifelse(gt, 1, 0))
  if (as.text){
    nd <- factor(nd, levels = -1:1, labels = c('<', '', '>'))
  }
  nd
}

parse.result <- function(x){
  annotations <- c("<", "EST", ">", "*")
  ret <- stri_replace_all_fixed(x, annotations, rep("", length(annotations)), vectorize_all = F)
  suppressWarnings(ret <- as.numeric(ret))
  ret
}

check.units <- function(unit, f){
  n.units <- vapply(split(unit, f), dplyr::n_distinct, FUN.VALUE = integer(1))
  if(any(n.units > 1)){
    warning("Some analytes have unit mismatches.")
  }
  n.units[n.units > 1]
}

# assign.units <- function(x, units){
#   sx <- split(x, units)
#   nsx <- names(sx)
#   ret <- lapply(nsx, function(i){
#     set_units(sx[[i]], i, mode = 'standard')
#   })
#
# }
#

#'@export
getProject <- function(..., dsn = NULL){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  where <- constructWhereStatement(..., start = NULL)
  query <- constructQuery('PROJECT', where, unrestricted = T)
  ret <- dbGetQuery(con, query)
  return(ret)
}

#'@export
getLocation <- function(..., dsn = NULL){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  where <- constructWhereStatement(..., start = NULL)
  query <- constructQuery('LOCATION', where, unrestricted = F)
  ret <- dbGetQuery(con, query)
  return(ret)
}

#'@export
getLocationByProject <- function(..., dsn = NULL){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  proj  <- getProject(...)
  query <- constructQuery('V_PROJECT_LOCATIONS', constructWhereStatement(project_id = proj$project_id, start = NULL), unrestricted = F)
  locs  <- dbGetQuery(con, query)
  ret   <- getLocation(location_id = locs$location_id)
  return(ret)
}

#'@export
getProjectByLocation <- function(..., dsn = NULL){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  locs  <- getLocation(...)
  query <- constructQuery('V_PROJECT_LOCATIONS', constructWhereStatement(location_id = locs$location_id, start = NULL), unrestricted = F)
  proj  <- dbGetQuery(con, query)
  ret   <- getProject(project_id = proj$project_id)
  return(ret)
}

#'@export
getSample <- function(..., start = NULL, end = NULL, date.field = 'sample_end_time', dsn = NULL){
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  table  <- 'JANUS_ELEMENT'#"V_RPT_JANUS_ELEMENT"
  fields <- c("location_code", "sample_code", "sample_name", "sample_point", "sample_begin_time",
              "sample_end_time", "sample_type", "matrix")
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = date.field)
  query <- constructQuery(table, where, unrestricted = F,
                          fields = fields, distinct = T)
  ret <- dbGetQuery(con, query)
  return(ret)
}
