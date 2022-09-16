#' Read from spatial data from City of Portland spatial databases
#'
#' Reads from a defined list of databases.
#'
#'@param database Either EGH_PUBLIC on GISDB1 or SWSP on BESDBPROD1
#'@param layer a fully qualified SQL table name, `[schema].[table]`
#'@param query a query to submit directly to server; `Shape.STAsBinary() as geometry` is necessary in select statement
#'@param ... additional arguments passed to st_read. The correct CRS, usually 2913, will need to be passed.
#'@returns an `sf` object, additionally inheriting `tbl_df`, `tbl`
#'@export
read.esri <- function(database = "EGH_PUBLIC", layer, query = NULL, ...){
  on.exit(dbDisconnect(con))
  con <- dbConnect(database)
  if(is.null(query)) {
    query <- glue::glue(
      "SELECT *, Shape.STAsBinary() as geometry
         FROM {layer}
         WHERE SHAPE IS NOT NULL;"
    )
  }

  sf::st_read(
    dsn             = con,
    geometry_column = "geometry",
    as_tibble       = TRUE,
    query           = query,
    ...
  )
}
