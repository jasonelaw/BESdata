dbConnect <- function(database = c('JANUS', 'NEPTUNE', 'WATERSHED'), server = NULL){
  database = match.arg(database)
  template <- "driver={ODBC Driver 17 for SQL Server};server=%s;database=%s;trusted_connection=YES"
  server.map   <- c(JANUS     = "BESREPORTS",
                    WATERSHED = "BESRPT1",
                    NEPTUNE   = "BESDBPROD1")
  if(is.null(server)) server <- server.map[database]
  conn.string <- sprintf(template, server, database)
  odbc::dbConnect(drv = odbc::odbc(), .connection_string = conn.string)
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
