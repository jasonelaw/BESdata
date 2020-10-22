#'Read point count data from Watershed database
#'
#'Reads point count data and survey information from BES watershed database
#'@param ... query parameters in \code{tag = value} form
#'@param start an object of class Date; the earliest survey start date to retrieve
#'@param end an object of class Date; the latest survey start date to retrieve
#'@param dsn Alternate dsn for the WATERSHED database - for access to production or test instances.
#'@examples
#'\dontrun{
#'#Get some data
#'d <- read.bird(start = as.Date('2017-01-01'))
#'}
#'@describeIn read.bird Read point count data
#'@export
read.bird <- function (..., start = NULL, end = NULL, dsn = NULL){
  on.exit(dbDisconnect(con))

   kView <- "V_RPT_AVIAN_POINT_COUNT_SPECIES_DATA"
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = "survey_start")
  query <- constructQuery(kView, where, unrestricted = TRUE)
  con <- connectWatershed(dsn)
  ret <- dbGetQuery(con, query)
  return(ret)
}

#'@describeIn read.bird Read point count survey information
#'@export
read.bird.survey <- function (..., start = NULL, end = NULL, dsn = NULL,
                              date.field = "survey_start"){
  on.exit(dbDisconnect(con))
  kView <- "V_RPT_AVIAN_POINT_COUNT_SURVEY"
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = "survey_start")
  query <- constructQuery(kView, where, unrestricted = TRUE)
  con <- connectWatershed(dsn)
  ret <- dbGetQuery(con, query)
  return(ret)
}
