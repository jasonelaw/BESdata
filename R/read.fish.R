#'Query and Read PAWMAP Fish Data
#'
#'Query PAWMAP electrofishing data and read the data into R.
#'
#'The function accepts a \code{start} and \code{end} date along
#'with query arguments in the form of \code{tag = value} statements
#'which correspond to valid fields in the database view (WATERSHED.V_RPT_WATERSHED)
#'used to access the data. The valid fields are: site_identifier, collection_start_date,
#'collection_end_date, fished, fished_none_collected, reviewed_by, reviewed_by_date,
#'fish_survey_update_by, fish_survey_update_date, field_shorthand_name,
#'common_name, taxon_name, fish_length_mm, clipped, mortality, anomaly,
#'fish_measure_update_by, and fish_measure_update_date. The start and end
#'parameters are queried against the collection_start_date field.
#'
#'The user running R must have permission to access the database view and
#'must have a ODBC Data Source set up for the view on the current computer.
#'The reporting server version of the view is currently BESREPORTS.WATERSHED.V_RPT_WATERSHED.
#'In addition, different ODBC connections must be set up for 32 and 64 bit
#'versions of R. The \code{RODBC::odbcDataSources()} function can be used to
#'check the currently available data sources from within R.
#'
#'@param ... query parameters in \code{tag = value} form
#'@param start an object of class Date; the earliest survey start date to retrieve
#'@param end an object of class Date; the latest survey start date to retrieve
#'@param dsn The data source name for the ODBC connection on the current computer.
#'@examples
#'d <- read.fish(start = as.Date('2017-01-01'), end = as.Date('2017-03-01'))
#'@export
read.fish <- function(..., start = end - 7, end = Sys.Date(), dsn = "WATERSHED_REP_64"){
  on.exit(dbDisconnect(con))
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = 'collection_start_date')
  kFishView     <- "V_RPT_WATERSHED"
  con   <- dbConnect(dsn)
  query <- constructQuery(kFishView, where, unrestricted = TRUE)
  ret   <- dbGetQuery(con, query)
  return(ret)
}
