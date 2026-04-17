besgeorpt <- function(server = "BESDB1") {
  connect(server,
          database = "BESGEORPT",
          timezone = "America/Los_Angeles",
          timezone_out = "America/Los_Angeles"
  )
}


layer <- function(connection, layer, ..., .crs, .collect = FALSE){
  ret <- dplyr::tbl(connection, layer) |>
    mutate(geometry = Shape.STAsBinary()) |>
    relocate(-Shape)
  attr(ret, ".crs") <- .crs
  if (.collect) {
    ret <- query_collect(ret)
  }
  ret
}
