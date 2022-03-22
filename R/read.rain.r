#'Retrieve rain data from City of Portland Hydra network.
#'
#'The City of Portland operates a rain gauge network.  This function retrieves
#'rain data from the SQL server database using the USP_MODEL_RAIN stored procedure.
#'The stored procedure summarizes the data within a prespecified period of time
#'(e.g., hourly) and returns the summarized data rather than raw tips.
#'@param station a vector of hydra station codes; the default is the WPCL gauge
#'@param start the start date as a \code{Date} object
#'@param end the end date as a \code{Date} object
#'@param daypart the unit in which the interval is specified
#'@param interval the interval over which the data should be summarized
#'@param dsn Alternate dsn for the NEPTUNE database - for access to production or test instances.
#'@param format logical, TRUE will perform some data formatting, FALSE will return the data.frame exactly
#'as it was returned by the database
#'@return a data.frame of rain data
#'@export
read.rain <- function(station = 160, start = end - 7, end = Sys.Date(),
                      daypart = c('day','hour', 'minute', 'month', 'year'),
                      interval = 1, server = NULL, format = T){
  con <- dbConnect(database = 'NEPTUNE', server = server)
  on.exit(dbDisconnect(con))
  stopifnot(lubridate::is.Date(start), lubridate::is.Date(end))
  make.queries <- function(start, end, interval, daypart, station){
    qry <- sprintf("{call USP_MODEL_RAIN('%s', '%s', %s, '%s', %s)}",
                   format(start), format(end), interval, daypart, station)
    ans <- dbGetQuery(con, qry)
    return(if (is.data.frame(ans)) ans else NULL)
  }


  # Format args and get data
  daypart <- match.arg(daypart)
  args <- data.frame(station, start, end, daypart, interval)
  rain <- purrr::pmap_dfr(args, make.queries)
  if (format){
    formatRain(rain, interval, daypart)
  } else {
    rain
  }
}


#'@importMethodsFrom lubridate +
calculateEndTime <- function(x, interval, daypart){
  x$start.utc + lubridate::period(interval, daypart)
}

#'@importMethodsFrom lubridate +
formatRain <- function(x, interval, daypart, local.tz = "America/Los_angeles"){
  x        <- merge(x, stations(), by.x = 'h2_number',
                       by = 'station', all.x = T)
  names(x) <- gsub('_', '.', names(x), fixed = T)
  x$rainfall.amount.inches <- as.numeric(x$rainfall.amount.inches)
  x$downtime               <- x$downtime == 'Y'
  x$sensor.present         <- x$sensor.present == 'Y'
  #   Fix dates
  x$start.local   <- parseUTCm8Time(x$date.time)
  x$end.local     <- x$start.local + lubridate::duration(interval, daypart)
  x$date.time   <- NULL
  #   Sort
  kFields <- c("station.name", "h2.number", "station.start", "station.end",
               "x", "y",
               "start.local", "end.local", "rainfall.amount.inches",
               "sensor.present", "downtime",  "station.id")
  x <- plyr::arrange(x, h2.number, start.local)
  x <- x[,kFields]
  class(x) <- c('intervalrain', 'data.frame')
  return(x)
}

stations <- function(server = NULL, local.tz = 'America/Los_angeles')
{
  con <- dbConnect(database = 'NEPTUNE', server = server)
  on.exit(dbDisconnect(con))
  qry <- c("
           SELECT DISTINCT STATION.h2_number as station,
                           STATION.station_id,
                           STATION.station_name,
                           STATION.state_plane_x_ft AS x,
                           STATION.state_plane_y_ft AS y,
                           STATION.start_date       AS station_start,
                           STATION.end_date         AS station_end
           FROM STATION INNER JOIN RAIN_SENSOR on STATION.station_id = RAIN_SENSOR.station_id;")
  res <- dbGetQuery(con, qry)

  res$station_start <- parseUTCm8Time(res$station_start)
  res$station_end   <- parseUTCm8Time(res$station_end)
  names(res) <- gsub('_', '.', names(res), fixed = T)
  res
}


