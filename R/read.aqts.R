#' Read location data from Aquarius
#'
#' This function uses the [raquarius::GetLocationDescriptionList()] function to retrieve location information from the AQTS Publish Service. Setting `full_metadata = TRUE` will return an [sf::st_sf] object which will allow easy plotting on a map or spatial filtering.
#' @param LocationName Filter results to the given location name
#' @param LocationIdentifier  Filter results to given location identifier
#' @param TagKeys Filter results to locations matching all tags by key.
#' @export
#' @examples
#' \dontrun{
#' # Get a list of all locations
#' read.aqts.locations()
#' # Get all locations matching a tag
#' loc_list <- read.aqts.locations(TagKeys = "PAWMAP")
#' # Get all metadata for those locations - somewhat slower as each location is a separate API request.
#' read.aqts.locations(TagKeys = "PAWMAP", full_metadata = TRUE)
#' }
#'
read.aqts.locations <- function(..., full_metadata = FALSE) {
  ret <- raquarius::GetLocationDescriptionList(..., .format = TRUE, .perform = TRUE)
  if(identical(full_metadata, TRUE)) {
    ret <- ret |>
      dplyr::select(LocationIdentifier = Identifier) |>
      raquarius:::aqts_perform_parallel(raquarius::GetLocationData)
    ret <- sf::st_as_sf(ret, coords = c("Longitude", "Latitude"), crs = 4326)
  }
  dplyr::rename_with(ret, snakecase::to_snake_case)
}

#' Read Time Series Information from Aquarius Time Series Publish Service
#'
#' `read.aqts.ts` is used to retrieve time series metadata from the AQTS Publish Service. Arguments to this function are passed unmodified to [raquarius::GetTimeSeriesUniqueIdList()] to retrieve matching time series. Then, metadata is retrieved with [raquarius::GetTimeSeriesDescriptionListByUniqueId()].
#'
#' @param LocationIdentifier A monitoring location identifier to filter by
#' @param Parameter A parameter to filter by
#' @export
#' @examples
#' \dontrun{
#' ret <- read.aqts.ts(
#'  LocationIdentifier = c("VNB", "21B"),
#'  Parameter = "Temperature",
#'  QueryFrom = Sys.date() - 365
#' )
#' head(ret)
#' ret <- ret |> unnest(points)
#' head(ret)
#' }
read.aqts.ts <- function(...) {
  ids <- raquarius::GetTimeSeriesUniqueIdList(..., .format = TRUE, .perform = TRUE)
  ret <- raquarius::GetTimeSeriesDescriptionListByUniqueId(
    TimeSeriesUniqueIds = ids$UniqueId,
    .format = TRUE,
    .perform = TRUE
  )
  dplyr::rename_with(ret, snakecase::to_snake_case)
}

#' Read Time Series data from Aquarius Time Series Publish Service
#'
#' `read.aqts.data` is used to retrieve time series data from the AQTS Publish Service. Arguments to this function are passed unmodified to [raquarius::GetTimeSeriesData()] if `.time_align = TRUE` or [raquarius::GetTimeSeriesCorrectedData] if `.time_align = FALSE`. Note, each of those functions takes slightly different argument values.
#' @param ... Arguments passed to [raquarius::GetTimeSeriesData()] if `.time_align = TRUE` or [raquarius::GetTimeSeriesCorrectedData] if `.time_align = FALSE`. Note, the arguments
#' @param .time_align If `TRUE`, the function will return multiple time aligned time series. If `FALSE`, will return one time series at a time with timestamps matching the original logged values for each series - which may not be time aligned.
#' @returns A `data.frame` with time series data.
#' @export
read.aqts.tsdata <- function(..., .time_align = TRUE) {
  ret <- raquarius::GetTimeSeriesData(..., .format = TRUE, .perform = TRUE)
  ret <- dplyr::rename_with(ret, snakecase::to_snake_case)
  ret$points <- map(ret$points, \(x) dplyr::rename_with(x, snakecase::to_snake_case))
  ret
}
