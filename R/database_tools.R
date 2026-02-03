

getDriver <- function(){
  drivers <- unique(odbc::odbcListDrivers()$name)
  if("ODBC Driver 17 for SQL Server" %in% drivers) {
    driver <- "ODBC Driver 17 for SQL Server"
  } else {
    driver <- "SQL Server"
  }
  return(driver)
}

dbConnect <- function(database, server){
  #c("JANUS", "NEPTUNE", "WATERSHED", "EGH_PUBLIC", "SWSP", "MODELING")
  #database <- match.arg(database)
  info <- list(
    JANUS      = list(database = "JANUS",      server = "BESRPT2"),
    NEPTUNE    = list(database = "NEPTUNE",    server = "BESDBPROD1"),
    WATERSHED  = list(database = "WATERSHED",  server = "BESRPT1"),
    EGH_PUBLIC = list(database = "EGH_PUBLIC", server = "GISDB1"),
    SWSP       = list(database = "SWSP",       server = "BESDBPROD1"),
    MODELING   = list(database = "MODELING",   server = "BESDBPROD1")
  )
  if (rlang::is_missing(server)) {
    rlang::check_required(database)
    rlang::arg_match(database, names(info))
    db.info <- info[[database]]
  } else {
    rlang::check_required(database)
    rlang::check_required(server)
    db.info <- list(
      database = database,
      server = server
    )
  }
  db.info$driver <- getDriver()
  template <- "driver={{{driver}}};server={server};database={database};trusted_connection=YES"
  odbc::dbConnect(
    drv = odbc::odbc(),
    .connection_string = glue::glue_data(db.info, template)
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
