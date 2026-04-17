
neptune <- function(server ="BESDB1") {
  connect(server,
    database = "NEPTUNE",
    timezone = "Etc/GMT+8",
    timezone_out = "Etc/GMT+8"
  )
}


rain <- function(connection, location, start, end,
    daypart = c('day','hour', 'minute', 'month', 'year'),
    interval = 1
  ) {
  daypart <- match.arg(daypart)
  params <- data.frame(
    start = format(start),
    end = format(end),
    interval = interval,
    daypart = daypart,
    station = location,
    limit_rows = 0L,
    adjust_for_dst = 0L
  )
  sql <- r"({CALL USP_MODEL_RAIN(?, ?, ?, ?, ?, ?, ?)})"
  ret <- odbc::dbGetQuery(
    conn = connection,
    statement = sql,
    params = params
  )
  ret |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      rainfall_amount_inches = units::set_units(rainfall_amount_inches, "in"),
      location_identifier = sprintf("HYDRA-%s", h2_number),
      sensor_present = sensor_present == "Y",
      downtime = downtime == "Y"
    ) |>
    dplyr::select(location_identifier, timestamp = date_time, rainfall_amount = rainfall_amount_inches, downtime, sensor_present, h2_number)
}

virtual_rain <- function(connection, location, start, end,
                 daypart = c('day','hour', 'minute', 'month', 'year'),
                 interval = 1
) {
  daypart <- match.arg(daypart)
  params <- data.frame(
    start = format(start),
    end = format(end),
    x_coord = x,
    y_coord = y,
    interval = interval,
    adjust_for_dst = FALSE,
    include_zeros = TRUE
  )
  sql <- r"({CALL USP_MODEL_VIRTUAL_RAIN(?, ?, ?, ?, ?, ?, ?)})"
  ret <- odbc::dbGetQuery(
    conn = connection,
    statement = sql,
    params = params
  )
  ret |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      rainfall_amount_inches = units::set_units(rainfall_amount_inches, "in"),
      location_identifier = sprintf("HYDRA-%s", h2_number),
      sensor_present = sensor_present == "Y",
      downtime = downtime == "Y"
    ) |>
    dplyr::select(location_identifier, timestamp = date_time, rainfall_amount = rainfall_amount_inches, downtime, sensor_present, h2_number)
}

# " @start_time = " + "'" + start_date + "'" + "," +\
# " @end_time = " + "'" + end_date + "'" + "," +\
# " @x_coord =" + str(x) + "," +\
# " @y_coord =" + str(y) + "," +\
# " @interval = " + str(interval) + "," + \
# " @include_zeros = true"
# USP_MODEL_VIRTUAL_RAIN


rain_locations <- function(connection, ..., .collect = FALSE) {
  fields <- c(
    "location_identifier",
    location_name = "station_name",
    location_description = "location_description",
    start_time = "rain_sensor_first_date",
    end_time = "rain_sensor_last_date",
    "is_active",
    x = "state_plane_x_ft",
    y = "state_plane_y_ft",
    h2_number = "h2_number",
    location_id = "station_id"
  )
  ret <- dplyr::tbl(connection, "V_QL_RAIN_SENSOR_FEATURE_CLASS") |>
    dplyr::mutate(
      location_identifier = str_c("HYDRA-", as.character(h2_number)),
      is_active = as.logical(rain_sensor_active == "Yes")
    )
  ret <- query_template(ret, ..., .fields = fields, .simple = TRUE, .collect = .collect)
  attr(ret, ".crs") <- 2913L
  attr(ret, ".coords") <- c("x", "y")
  if(.collect) {
    ret <- query_collect(ret)
  }
  ret
}
