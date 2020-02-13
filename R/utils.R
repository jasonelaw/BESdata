compose <- function (...){
  fs <- lapply(list(...), match.fun)
  n <- length(fs)
  last <- fs[[n]]
  rest <- fs[-n]
  function(...) {
    out <- last(...)
    for (f in rev(rest)){
      out <- f(out)
    }
    out
  }
}

constructQuery <- function(table, where, unrestricted, fields = '*', distinct = F){
  stopifnot(is.character(table), is.character(where), is.logical(unrestricted))
  is.unrestricted <- identical(stringi::stri_length(where), 0L)
  if(is.unrestricted && !unrestricted){
    stop("The where argument cannot be an empty string when unrestricted = FALSE.")
  }
  qry <- if(is.unrestricted) "SELECT %s%s FROM %s%s;" else "SELECT %s%s FROM %s WHERE %s;"
  query <- sprintf(qry,if(distinct) "DISTINCT " else "", toString(fields), table, where)
  query
}

ifApplyFun <- function(cFUN, FUN1, FUN2 = identity){
  function(x, ...){
    if(cFUN(x)) FUN1(x, ...) else FUN2(x)
  }
}

formatDate <- ifApplyFun(lubridate::is.instant, format)

quoteChar  <- ifApplyFun(is.character, sQuote)

is.length1 <- function(x) identical(length(x), 1L)

textToSQL <- function(x){
  fmt <- if (is.length1(x)) "= %s" else "IN (%s)"
  sprintf(fmt, stringi::stri_join(x, collapse = ', '))
}

dateToSQL <- function(x, op = c(">", "<")){
  op <- match.arg(op)
  date.str <- compose(quoteChar, formatDate)(x)
  sprintf("%s %s", op, date.str)
}

constructWhereStatement <- function(..., start = Sys.Date() - 30, end = NULL,
                                    date.field = NULL){
  pars <- list(...)
  isnull.start  <- is.null(start)
  isnull.end    <- is.null(end)
  isnull.dfield <- is.null(date.field)
  if(any(!isnull.start, !isnull.end) & isnull.dfield){
    stop("The 'date.field' argument cannot be NULL when a date argument ('start' or 'end') is provided.", call. = FALSE)
  }
  if(any(duplicated(names(pars)))){
    stop('Multiple arguments with the same name are not allowed.', call. = FALSE)
  }
  stopifnot(lubridate::is.Date(start) | is.null(start),
            lubridate::is.Date(end)   | is.null(end))
  op <- options()
  on.exit(options(op))
  options(useFancyQuotes = FALSE)
  pars <- lapply(pars, compose(textToSQL, quoteChar))
  if(!is.null(date.field)){
    date.pars <- structure(list(start = dateToSQL(start, ">"),
                                end   = dateToSQL(end,   "<")),
                           names = rep(date.field, 2))
    pars <- c(pars, date.pars)
  }
  pars <- c(pars, recursive = TRUE)
  paste(names(pars), pars, sep = ' ', collapse = " AND ")
}

# Fix times: times in NEPTUNE are UTC-08:00.  Create local times from these.
parseUTCm8Time <- function(x, tz = "America/Los_angeles"){
  x <- stringi::stri_join(x, " -08:00")
  lubridate::ymd_hms(x, quiet = TRUE, tz = tz)
}

parseLocalTime <- function(x){
  lubridate::ymd_hms(x, quiet = TRUE, tz = 'America/Los_angeles')
}

#'@import stringi
#'@import checkmate
formatDataFrame <- function(x, drop = NULL, sort = NULL,
                            date = NULL, numeric = NULL, bool = NULL, true.value = NULL,
                            parseDate = parseLocalTime){
  checkmate::assertClass(x, 'data.frame')
  nms <- names(x)
  checkmate::assertSubset(c(drop, sort, date, bool), choices = nms)

  # parse dates
  #local <- stri_join(utcm8, '.local')
  x[date]    <- lapply(x[date], parseDate)
  x[numeric] <- lapply(x[numeric], as.numeric)
  x[bool]    <- lapply(x[bool], '==', true.value)
  x[] <- lapply(x, function(x) if (is.character(x)) stringi::stri_trim_both(x) else x)
  if(!is.null(sort)){
    x <- x[do.call(order, x[,sort]), ]
  }
  if(!is.null(drop)){
    x <- x[, setdiff(nms, drop)]
  }
  row.names(x) <- 1:nrow(x)
  x
}
