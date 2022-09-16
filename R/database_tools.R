getDriver <- function(){
  drivers <- unique(odbc::odbcListDrivers()$name)
  if ("ODBC Driver 18 for SQL Server" %in% drivers){
    driver <- "ODBC Driver 18 for SQL Server"
  } else if("ODBC Driver 17 for SQL Server" %in% drivers) {
    driver <- "ODBC Driver 17 for SQL Server"
  } else {
    driver <- "SQL Server"
  }
  return(driver)
}

dbConnect <- function(database = c("JANUS", "NEPTUNE", "WATERSHED", "EGH_PUBLIC", "SWSP"), server = NULL){
  database <- match.arg(database)
  info <- list(
    JANUS      = list(database = "JANUS",      server = "BESRPT2"),
    NEPTUNE    = list(database = "NEPTUNE",    server = "BESDBPROD1"),
    WATERSHED  = list(database = "WATERSHED",  server = "BESRPT1"),
    EGH_PUBLIC = list(database = "EGH_PUBLIC", server = "GISDB1"),
    SWSP       = list(database = "SWSP",       server = "BESDBPROD1")
  )
  db.info <- info[[database]]
  db.info$driver <- getDriver()
  if(!is.null(server)) {
    db.info$server <- server
  }
  template <- "driver={{{driver}}};server={server};database={database};trusted_connection=YES"
  odbc::dbConnect(
    drv = odbc::odbc(),
    .connection_string = glue::glue(template, .envir = db.info)
  )
}

dbDisconnect <- function(conn){
  odbc::dbDisconnect(conn)
}

dbGetQuery <- function(conn, statement){
  ret <- odbc::dbGetQuery(conn, statement)
  if((is.data.frame(ret) && nrow(ret) < 1)){
    warning("There was no data in the query.")
  }
  return(ret)
}

dbReadTable <- function(conn, name){
  ret <- odbc::dbReadTable(conn, name)
  if(nrow(ret) < 1){
    warning("There was no data in the query.")
  }
  return(ret)
}
