#'Query and Read PAWMAP Fish Data
#'
#'Query PAWMAP electrofishing data and read the data into R.
#'
#'The function accepts a \code{start} and \code{end} date along
#'with query arguments in the form of \code{tag = value} statements
#'which correspond to valid fields in the database view (WATERSHED.V_RPT_WATERSHED)
#'used to access the data. The start and end
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
#'@param dsn Alternate dsn for the WATERSHED database - for access to production or test instances.
#'@examples
#'\dontrun{
#'#List the available fields
#'library(RODBC)
#'con <- BESdata:::dbConnect("WATERSHED")
#'sqlColumns(con, "V_RPT_WATERSHED")
#'#Get some data
#'d <- read.fish(start = as.Date('2017-01-01'), end = as.Date('2017-03-01'))
#'}
#'@export
read.fish <- function(..., start = end - 7, end = Sys.Date(), dsn = NULL){
  on.exit(dbDisconnect(con))
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = 'collection_start_date')
  kFishView     <- "V_RPT_WATERSHED"
  con <- if(is.null(dsn)){ dbConnect(database = 'WATERSHED') } else { dbConnect(database = 'DSN', dsn = dsn) }
  query <- constructQuery(kFishView, where, unrestricted = TRUE)
  ret   <- dbGetQuery(con, query)
  return(ret)
}
