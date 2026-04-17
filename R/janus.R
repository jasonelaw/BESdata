
janus <- function(server = "BESRPT2") {
  dbConnect(server,
    database = "JANUS",
    timezone = "America/Los_Angeles",
    timezone_out = "America/Los_Angeles"
  )
}


chemistry <- function(connection, ..., .simple = TRUE, .collect = FALSE) {
  fields <- c(
    "project_name", "location_code",
    "sample_code", "sample_name", "sample_type", "matrix",
    start_time = "sample_begin_time", end_time = "sample_end_time",
    "janus_analyte_name", "combined_result",
    "analyte_units", "method_code"
  )
  ret <- dplyr::tbl(connection, "JANUS_ELEMENT") |>
    dplyr::select(
      project_name, location_identifier = location_code,
      sample_code, sample_name, sample_type, matrix,
      start_time = sample_begin_time, end_time = sample_end_time,
      analyte_name = janus_analyte_name, combined_result,
      analyte_units, method_code
    ) |>
    dplyr::mutate(
      nd = dplyr::case_when(
        stringr::str_sub(combined_result, 1, 1) == "<" ~ -1,
        stringr::str_sub(combined_result, 1, 1) == ">" ~ 1,
        TRUE ~ 0
      ),
      result = as.numeric(TRANSLATE(combined_result, "<>EST*", "      "))
    ) |>
    dplyr::filter(...)
  if(.collect) {
    ret <- query_collect(ret)
  }

  ret
}


janus_locations <- function(connection, ..., .simple = TRUE, .collect = FALSE) {
  ret <- dplyr::tbl(connection, "V_LOCATION_LIST") |>
    dplyr::select(
      location_identifier = location_code,
      location_type = location_type_bes_name,
      location_description,
      access_notes, safety_notes,
      hansen_id = manhole_hansen_id,
      comppkey = pipe_hansen_compkey,
      distance_from_manhole = distance_from_manhole_inches,
      datum_lat_long,
      has_flow = is_flow_location,
      is_active_flow = active_flow_monitoring_location,
      outfall_basin_number,
      location_id,
      longitude,
      latitude
    ) |>
    dplyr::mutate(
      has_flow = has_flow == "Y",
      is_active_flow = is_active_flow == "Y"
    ) |>
    dplyr::filter(...)
  attr(ret, ".crs") <- 2913L
  attr(ret, ".coords") <- c("longitude", "latitude")
  if (.collect) {
    ret <- query_collect(ret)
  }
  ret
}


janus_flow <- function(connection, ..., .collect = FALSE) {
  mv <- dplyr::tbl(connection, "V_METER_VISIT_LIST_NARROW") |>
    dplyr::select(
      project_name, location_identifier = location_code, hansen_id = manhole_hansen_id,
      location_description, meter_visit_id
    )
  mr <- dplyr::tbl(connection, "METER_READING") |>
    dplyr::select(
      timestamp = reading_datetime, depth = depth_inches, velocity = velocity_fps,
      flow = flow_cfs_AxV, depth_qualifier, velocity_qualifier, meter_visit_id
    )
  ret <- mr |>
    dplyr::left_join(mv, by = dplyr::join_by(meter_visit_id == meter_visit_id))
  ret <- query_template(ret, ..., .fields = fields, .simple = FALSE, .collect = .collect)
  if(.collect) {
    ret <- ret |>
      dplyr::mutate(
        depth    = units::set_units(depth, "in", mode = "standard"),
        velocity = units::set_units(velocity, "ft/s", mode = "standard"),
        flow     = units::set_units(flow, "ft^3/s", mode = "standard")
      )
  }
  ret
}

