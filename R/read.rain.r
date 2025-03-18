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
formatRain <- function(x, interval, daypart, local.tz = Sys.timezone()){
  kFields <- c(
    "location_id", "location_name",
    "start_local", "end_local",
    "rainfall_amount_inches",
    "sensor_present", "downtime",
    "h2_number"
  )
  l <- read.rain.locations()
  i <- match(x$h2_number, l$h2_number)
  x$location_name <- l$location_name[i]
  x$location_id   <- l$location_id[i]
  x$rainfall_amount_inches <- as.numeric(x$rainfall_amount_inches)
  x$downtime       <- x$downtime == "Y"
  x$sensor_present <- x$sensor_present == "Y"
  x$start_local    <- parseUTCm8Time(x$date_time)
  x$end_local      <- x$start_local + lubridate::duration(interval, daypart)
  x[, kFields]
}

#'@rdname read.rain
#'@export
read.rain.locations <- function(server = NULL)
{
  con <- dbConnect(database = 'NEPTUNE', server = server)
  on.exit(dbDisconnect(con))
  qry <- c("
    SELECT
      station_id AS location_id,
      h2_number,
      station_name AS location_name,
      location_description,
      state_plane_x_ft AS x,
      state_plane_y_ft AS y,
      start_date AS station_start_date,
      end_date AS station_end_date,
      station_active AS location_active,
      rain_sensor_active,
      rain_sensor_first_date,
      rain_sensor_last_date
    FROM V_RAIN_SENSOR_FEATURE_CLASS;
  ")

  res <- dbGetQuery(con, qry)
  res$rain_sensor_first_date <- parseUTCm8Time(res$rain_sensor_first_date)
  res$rain_sensor_last_date  <- parseUTCm8Time(res$rain_sensor_last_date)
  res$station_start_date <- parseUTCm8Time(res$station_start_date)
  res$station_end_date   <- parseUTCm8Time(res$station_end_date)
  res <- sf::st_as_sf(
    x = tibble::as_tibble(res),
    coords = c("x", "y"),
    crs = 2913
  )
  res
}

# "
#   SELECT
#     STATION.station_id AS location_id,
#     STATION.h2_number as h2_number,
#     station_name AS location_name,
#     state_plane_x_ft AS x,
#     state_plane_y_ft AS y,
#     RHS.start_date AS start_date,
#     RHS.end_date AS end_date
#   FROM STATION
#   INNER JOIN (
#     SELECT
#       station_id,
#       MIN(start_date) AS start_date,
#       MAX(end_date) AS end_date
#     FROM RAIN_SENSOR
#     GROUP BY station_id
#   ) RHS
#     ON (STATION.station_id = RHS.station_id)
#   ")
