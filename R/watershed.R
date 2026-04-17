
watershed <- function(server = "BESRPT2") {
  connect(server,
    database = "WATERSHED",
    timezone = "America/Los_Angeles",
    timezone_out = "America/Los_Angeles"
  )
}


fish <- function(connection, ..., .collect = FALSE) {
  ret <- dplyr::tbl(connection, "V_RPT_WATERSHED") |>
    dplyr::mutate(
      effort = fished,
      was_fished = str_sub(fished, 1, 6) == "Fished",
      any_collected = fished_none_collected == "No" & was_fished,
      is_clipped = clipped == "Yes",
      anomaly = anomaly == "Yes"
    ) |>
    dplyr::select(
      location_identifier = site_identifier,
      start_time = collection_start_date,
      end_time = collection_end_date,
      was_fished, any_collected,
      effort,
      taxon_name,
      fish_length = fish_length_mm,
      common_name,
      field_name = field_shorthand_name,
      is_clipped, anomaly
    )
  ret <- query_template(ret, ..., .simple = FALSE, .collect = .collect)
  if (.collect) {
    ret <- ret |>
      dplyr::mutate(
        fish_length = units::set_units(fish_length, "mm", mode = "standard")
      )
  }
  ret
}


avian <- function(connection, ..., .collect = FALSE) {
  rename <- \(x) stringr::str_replace(x, "avian_|_name", "")
  ret <- dplyr::tbl(connection, "V_RPT_AVIAN_POINT_COUNT_SPECIES_DATA") |>
    dplyr::mutate(
      is_flock = is_flock_name == "Yes",
      is_juvenile = is_juvenile_name == "Yes"
    ) |>
    dplyr::select(
      project_name = avian_project_name,
      protocol,
      location_identifier = site_identifier,
      start_time = survey_start,
      radius = historical_fixed_distance_radius,
      distance = distance_meter,
      time_interval = avian_time_bracket_name,
      species_code, common_name, number_observed,
      detection_code = avian_detection_code_name,
      detection_type = avian_detection_type_name,
      notes,
      is_flock, is_juvenile
    )
  ret <- query_template(ret, ..., .simple = FALSE, .collect = .collect)
  if (.collect) {
    ret <- ret |>
      dplyr::mutate(
        distance = units::set_units(distance, "m", mode = "standard")
      )
  }
  ret
}


taxa <- function(connection, ..., .collect = FALSE) {
  ret <- dplyr::tbl(connection, "V_SPECIES_ALL_SEARCH")
  ret <- query_template(ret, ..., .simple = FALSE, .collect = .collect)
  ret
}

habitat <- function(connection, ..., .collect = FALSE) {
  ret <- dplyr::tbl(connection, I("output.V_RPT_METRIC_HABITAT"))
  ret <- query_template(ret, ..., .simple = FALSE, .collect = .collect)
  ret
}
