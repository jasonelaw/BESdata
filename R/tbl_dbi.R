connect <- function(server, database, method = odbc::dbConnect, ...) {
  driver <- getDriver()
  template <- "driver={<driver>};server=<server>;database=<database>;trusted_connection=YES"
  con_str <- glue::glue(template, .open = "<", .close = ">")
  con <- odbc::dbConnect(odbc::odbc(), .connection_string = con_str, ...)
  con
}

query_template <- function(x, ..., .fields, .simple, .collect) {
  if(.simple) {
    x <- x |>
      dplyr::select(dplyr::any_of(.fields))
  }
  x <- dplyr::filter(x, ...)
  if (.collect) {
    x <- query_collect(x)
  }
  x
}

query_collect <- function(x, ...) {
  n <- x |> dplyr::count() |> dplyr::pull(n)
  if (n > 1e4) cli::cli_alert_info("Query will generate {n} rows")
  .coords <- attr(x, ".coords")
  ret <- dplyr::collect(x)
  if(!is.null(attr(x, ".crs"))) {
    if(!is.null(.coords)) {
      if(any(is.na(ret[, .coords]))){
        cli::cli_alert_warning("Missing values in coordinates - returning a `tibble` rather than `sf` object")
      } else {
        ret <- st_as_sf(ret, crs = attr(x, ".crs"), coords = .coords)
      }
    } else {
      ret <- st_as_sf(ret, crs = attr(x, ".crs"))
    }
  }
  ret
}
