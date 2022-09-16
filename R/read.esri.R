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
  #dbGetQuery(con, statement = glue::glue(query))
  sf::st_read(
    dsn             = con,
    geometry_column = "geometry",
    as_tibble       = TRUE,
    query           = query,
    ...
  )
}

# ESRI <- function(server, database, schema) {
#   drivers <- unique(odbcListDrivers()$name)
#   if ("ODBC Driver 17 for SQL Server" %in% drivers){
#
#   }
#   string <- "driver={{ODBC Driver 17 for SQL Server}};server={server};database={database};schema={schema};trusted_connection=YES"
#   glue::glue(string)
#   # db_conn <- DBI::dbConnect(
#   #   drv = odbc::odbc(),
#   #   driver = "SQL Server",
#   #   server = "GISDB1",
#   #   database = "EGH_PUBLIC",
#   #   schema = "ArcMap_Admin",
#   #   trusted_connection = "True"
#   # )
# }

# library(odbc)
# library(sf)
# con <- odbc::dbConnect(odbc::odbc(), .connection_string = ESRI())
# template <- "SELECT {toString(fields)}, Shape.STAsBinary() as [geometry] FROM {layer} WHERE 1 = 1;"
# layer <- "[ArcMap_Admin].[CENSUS_TRACTS_2010_PDX]"
# fields <- setdiff(dbListFields(con, "CENSUS_TRACTS_2010_PDX"), "Shape")
# qry <- glue::glue(template)
# tmp <- dbGetQuery(
#   conn = con,
#   statement = qry
# )
# tmp$geometry <- st_as_sfc(tmp$geometry)
# tmp2 <- st_as_sf(tmp, wkt = "geometry", crs = 2913)
