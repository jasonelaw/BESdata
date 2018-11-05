#'Retrieve flow data from Janus
#'
#'Submits a query to a table join allowing for queries by Hansen ID and Date
#'
#'To query by Hansen ID use the field name mv.manhole_hansen_id. To query a specific
#'date range use start and end arguments, otherwise all available data at that location
#'will be returned.
#'
#'The user running R must have permission to access the view (V_METER_VISIT_LIST_NARROW) and
#'and table (METER_READING) used in the query. The user must have
#'an ODBC Data Source set up for the Janus database on the current computer. The reporting
#'server version of the database is currently BESREPORTS.JANUS.
#'
#'
#'@param ... query arguments in the form 'field = value'
#'@param start start date of data range
#'@param end end date of data range
#'@param dsn Alternate dsn for the JANUS database - for access to production or test instances.
#'@param date.field the field used for the start and end query arguments. The default value is "reading_datetime"
#'@return a data frame of results with the fields: \cr
#'project_name\cr
#'location_code\cr
#'location_description\cr
#'manhole_hansen_id \cr
#'reading_datetime \cr
#'depth_inches \cr
#'velocity_fps \cr
#'depth_qualifier \cr
#'velocity_qualifier \cr
#'flow_cfs_AxV \cr
#'@export
#'@author Peter Bryant peter.bryant@portlandoregon.gov
read.flow <- function (..., start = NULL, end = NULL, dsn = NULL,
                       date.field = "reading_datetime")
{
  con <- if(is.null(dsn)){ dbConnect(database = 'JANUS') } else { dbConnect(database = 'DSN', dsn = dsn) }
  on.exit(dbDisconnect(con))
  where <- constructWhereStatement(..., start = start, end = end,
                                   date.field = date.field)
  query <- "
    SELECT  mv.project_name               AS project_name,
            mv.location_code              AS location_code,
            mv.location_description       AS location_description,
            mv.manhole_hansen_id          AS manhole_hansen_id,
            mr.reading_datetime           AS reading_datetime,
            mr.depth_inches               AS depth_inches,
            mr.velocity_fps               AS velocity_fps,
            mr.depth_qualifier            AS depth_qualifier,
            mr.velocity_qualifier         AS velocity_qualifier,
            mr.flow_cfs_AxV               AS flow_cfs_AxV
    FROM        METER_READING             AS mr
      LEFT JOIN V_METER_VISIT_LIST_NARROW AS mv
        ON  mr.meter_visit_id = mv.meter_visit_id
    WHERE %s"
  query <- sprintf(query, where)
  ret <- dbGetQuery(con, query)
  ret <- formatDataFrame(ret, date = 'reading_datetime', parseDate = parseUTCm8Time)
  ret[,c(6,7,10)] <- lapply(ret[,c(6,7,10)], as.numeric)
  ret$manhole_hansen_id <- as.factor(ret$manhole_hansen_id)
  return(ret)
}
