#' Storm delineation methods
#'
#' These two functions assign determine whether each time point in a time series
#' of rainfall totals is in a storm event or not. The \code{cso_storm} function
#' implements an algorithm that has been used to determine storm events for CSO
#' program reporting and analysis. The \code{pot_storm} method implements a more
#' general purpose peak over threshold method along with a minimum inter-event
#' time. Any rain value over the threshold is part of a storm and time stamps less
#' than \code{iet} distance from each other are considered part of the same storm.
#'
#' The \code{cso_stor} storm detection algorithm is documented in
#' OneNote and a pdf document maintained by the ASM team. Please contact them for
#' access to the most recent version. Jason Law has also documented the algorithm
#' in a memo with more detailed mathematical description of the algorithm.
#' The algorithm relies on start points that
#' occur when it is currently raining, a forward looking rolling sum is over
#' the threshold, and the previous time points within the IET
#' (i.e., (i-iet+1):(i-1) time points where the current point is i. A storm
#' continues if the previous value is a storm and the current value is not a
#' stopping point. The current value is a stopping point if, a backward sum is not over the
#' threshold and the forward
#'
#' @param x a vector of rain amounts
#' @param time a vector of time stamps, used to check order if present. If this
#' argument is not used, the vector of rain amounts \code{x} is assumed to be
#' ordered
#' @param threshold a scalar representing the method threshold - this parameter
#' has different interpretations for each delineation method
#' @param iet a scalar representing the minimum inter-event time duration - the
#' parameter has essentially the same meaning across methods, but the restriction
#' is enforced in very different ways and so the results will differ across methods
#' @name storm_delineation
NULL

#' @rdname storm_delineation
#' @export
cso_storm <- function(x, time, threshold = 0.1, iet = 10){
  if(missing(time)){
    warning("time is missing, ordering is not checked and data are assumed ordered")
  }
  if(!missing(time) && is.unsorted(time)){
    stop("Data is unordered!")
  }
  if (sum(x[1:iet], na.rm = TRUE) > 0){
    warning("Rain at start of data (%s in), initial storm IET may be inaccurate")
  }
  x[is.na(x)] <- 0                               # Excel treats empty as 0 - coerce missing to 0
  n <- length(x)
  At <- logical(n)                               # Active event - initialize to FALSE
  Ft <- RcppRoll::roll_suml(x, iet, fill  = 0) >= threshold # forward sum over threshold
  Bt <- RcppRoll::roll_sumr(x, iet, fill = 0) >= threshold  # backward sum over threshold
  Ct <- x  >  0                             # currently raining
  Ft_and_Ct  <- Ft & Ct #Ft * Ct            # starting point
  Ft_or_notBtm1 <- Ft | !lag1(Bt)           # not a stopping point
  for (i in (iet+2):n){
    # iet check index
    j <- (i-iet+1):(i-1)
    At[i] <- (At[i-1] && Ft_or_notBtm1[i]) || (Ft_and_Ct[i] && all(!At[j]))
  }
  At
}

cso_storm2 <- function(x, time, threshold = 0.1, iet = 10){
  if(missing(time)){
    warning("time is missing, ordering is not checked and data are assumed ordered")
  }
  if(!missing(time) && is.unsorted(time)){
    stop("Data is unordered!")
  }
  if (sum(x[1:iet]) > 0){
    warning("Rain at start of data (%s in), initial storm IET may be inaccurate")
  }
  x[is.na(x)] <- 0                               # Excel treats empty as 0 - coerce missing to 0
  n <- length(x)
  At <- integer(n)                               # Active event - initialize to FALSE
  Ft <- RcppRoll::roll_suml(x, iet, fill  = 0) >= threshold # forward sum over threshold
  Bt <- RcppRoll::roll_sumr(x, iet, fill = 0) >= threshold  # backward sum over threshold
  Ct    <- x  >  0                               # currently raining
  Ft.Ct  <- Ft * Ct                              # starting point
  Btm1 <- lag1(Bt)
  for (i in (iet+2):n){
    # iet check index
    j <- (i-iet+1):(i-1)
    # Is t-1 an event and is t not a stopping point
    # OR - It's a start point and no event in iet
    At[i] <- (At[i-1] * Btm1[i] +
                Ft.Ct[i] * all(!At[j])   +
                At[i-1] * Ft[i] * Btm1[i] +
                At[i-1]) %% 2
  }
  as.logical(At)
}
#Atm1*Btm1 + Ct*Pt*Ft + Atm1*Ft*Btm1 + Atm1) %% 2

#https://en.wikipedia.org/wiki/Algebraic_normal_form
#(Atm1 & (Ft | !Btm1)) | (Ft & Ct & Pt)
#Atm1=x;Ft=y;Btm1=z;Ct=a;Pt=b
#BooleanConvert[(x && (y || !z)) || (y && a && b), "ANF"]
#(x && (y || !z)) || (y && a && b)
#(x ∧ z) ⊻ (a ∧ b ∧ y) ⊻ (x ∧ y ∧ z) ⊻ (a ∧ b ∧ x ∧ y) ⊻ x
#xz + aby + xyz + abxy + x
#Atm1*Btm1 + Ct*Pt*Ft + Atm1*Ft*Btm1 + Ct*Pt*Atm1*Ft + Atm1

# At first I put it into Mathematica with !Btm1=z like BooleanConvert[(x && (y || z)) || (y && a && b), "ANF"]
#(x ∧ y) ⊻ (x ∧ z) ⊻ (a ∧ b ∧ y) ⊻ (x ∧ y ∧ z) ⊻ (a ∧ b ∧ x ∧ y)
# xy + xz + aby + xyz + abxy

cso_storm3 <- function(x, time, threshold = 0.1, iet = 10){
  if(missing(time)){
    warning("time is missing, ordering is not checked and data are assumed ordered")
  }
  if(!missing(time) && is.unsorted(time)){
    stop("Data is unordered!")
  }
  if (sum(x[1:iet]) > 0){
    warning("Rain at start of data (%s in), initial storm IET may be inaccurate")
  }
  x[is.na(x)] <- 0                               # Excel treats empty as 0 - coerce missing to 0
  n <- length(x)
  At <- integer(n)                               # Active event - initialize to FALSE
  Ft <- RcppRoll::roll_suml(x, iet, fill  = 0) >= threshold # forward sum over threshold
  Bt <- RcppRoll::roll_sumr(x, iet, fill = 0) >= threshold  # backward sum over threshold
  Ct    <- x  >  0                               # currently raining
  Ft.Ct  <- Ft * Ct                              # starting point
  Btm1 <- lag1(Bt)
  Ft_nBtm1 <- pmax(Ft, 1 - Btm1)
  for (i in (iet+2):n){
    # iet check index
    j <- (i-iet+1):(i-1)
    # Is t-1 an event and is t not a stopping point
    # OR - It's a start point and no event in iet
    At[i] <- max(Ft.Ct[i] * all(!At[j]), At[i-1] * Ft_nBtm1[i])
  }
  as.logical(At)
}

#' Accepts a boolean x, and a size argument. Looks for runs of true in boolean
#' and
decluster <- function(x, min.diff){
  ix   <- which(x)
  nix <- length(ix)
  idix <- which(diff(ix) > min.diff)
  iend <- ix[c(idix, nix)]
  isrt <- ix[c(1L, idix + 1L)]
  ret <- new_bounds(start = isrt, end = iend, size = vctrs::vec_size(x))
  bounds_to_bool(ret)
}

#' @name storm_delineation
#' @export
pot_storm <- function(x, time, threshold, iet){
  if(missing(time)){
    warning("time is missing, ordering is not checked and data are assumed ordered")
  }
  if(!missing(time) && is.unsorted(time)){
    stop("Data is unordered!")
  }
  pot <- x > threshold
  decluster(pot, iet)
}

#' @export
summarize_storms <- function(x, storm, time){
  if(inherits(storm, "logical")){
    storm <- bool_to_bounds(storm)
  }
  index  <- bounds_to_index(storm)
  start <- vctrs::field(storm, "start")
  f <- function(i){
    xi <- x[i]
    c(
      sum(xi, na.rm = T),
      which.max(xi),
      sum(is.na(xi))
    )
  }
  ret <- vapply(index, f, FUN.VALUE = setNames(numeric(3), c("sum", "which.max", "nas")))
  ret <- vctrs::vec_cbind(vctrs::new_data_frame(storm), t(ret))
  ret$which.max <- ret$which.max + start - 1
  ret$max <- x[ret$which.max]
  attr(ret, "total") <- sum(x, na.rm = TRUE)
  if(!missing(time)){
    ret <- to_explicit_time(ret, time)
  }
  ret
}

to_explicit_time <- function(x, t){
  total <- attr(x, "total")
  dt <- unique(as.double(diff(t), units = "secs"))
  stopifnot(identical(length(dt), 1L))
  x$period <- lubridate::interval(t[x$start], t[x$end])
  x$max_t <- t[x$which.max]
  x$duration <- lubridate::dseconds(dt) * x$n
  ret <- x[,c("id", "period", "duration", "sum", "max", "max_t", "nas")]
  attr(ret, "total") <- total
  ret
}

#' Summarize storm delineation for entire period
#'
#' This function summarizes the output of \code{summarize_storms}. It calculates
#' summaries like the total rainfall over the period, the amount contained in
#' the storms, the proportion of storm rainfall out of total rainfall,
#' total duration of storms, average intensity of storms extracted.
#'
#' This function can be used to summarize the performance of the storm
#' delineation methods (e.g., \link{pot_storm} or \link{cso_storm}) or different
#' parameter values passed to these methods
#'
#' @param storms A data.frame created using \link{summarize_storms}
#'
#' @return a single row data.frame with columns summarizing the performance of
#' the delineation
#' @export
summarize_delineation <- function(storms){
  total <- attr(storms, "total")
  storm_total <- sum(storms$sum)
  storm_len <- duration(sum(storms$duration))

  message(glue::glue("Delineated events with {storm_total} in / {total} in of rain in {storm_len}"))
  data.frame(
    total = total,
    storm_total = storm_total,
    proportion_in_storm = total/storm_total,
    storm_length = storm_len,
    avg_int = storm_total / (as.numeric(storm_len) / 60^2)
  )
}

# Helpers ----------------------------------------------------------------------

intmap <- function(a, b) if_else(a >= b, a * a + a + b, a + b * b)

lag1 <- function(x) c(FALSE, x[1:(length(x)-1)])

new_bounds <- function(start = integer(), end = integer(), size) {
  vctrs::vec_assert(start, integer())
  vctrs::vec_assert(end, integer())

  vctrs::new_rcrd(
    fields = list(
      start = start,
      end   = end,
      n     = end - start + 1,
      id    = seq_along(start)
    ),
    size = size,
    class = "bounds"
  )
}

#' @export
format.bounds <- function(x, ...) {
  start <- vctrs::field(x, "start")
  end <- vctrs::field(x, "end")
  sprintf("%s:%s", start, end)
}

bounds_to_index <- function(x){
  n     <- vctrs::field(x, "n")
  start <- vctrs::field(x, "start")
  vctrs::vec_chop(sequence(n, from = start), sizes = n)
}

bool_to_index <- function(x){
  rl <- vctrs::vec_unrep(x)
  sizes <- rl$times[rl$key]
  vctrs::vec_chop(which(x), sizes =  sizes)
}

bool_to_bounds <- function(x){
  rl <- vctrs::vec_unrep(x)
  end <- cumsum(rl$times)
  start <- end - rl$times + 1
  ret <- vctrs::vec_slice(data.frame(start, end), rl$key)
  new_bounds(as.integer(ret$start), as.integer(ret$end), vctrs::vec_size(x))
}

bounds_to_bool <- function(x){
  n     <- vctrs::field(x, "n")
  start <- vctrs::field(x, "start")
  ret <- logical(attr(x, "size"))
  vctrs::vec_assign(ret, sequence(n, from = start), value = TRUE)
}

#' @export
storm_id <- function(x){
  ret <- vctrs::vec_init(NA_integer_, vctrs::vec_size(x))
  bnds <- bool_to_bounds(x)
  start <- vctrs::field(bnds, "start")
  n     <- vctrs::field(bnds, "n")
  vctrs::vec_assign(
    x = ret,
    i =  sequence(n, from = start),
    value = rep.int(1:vctrs::vec_size(bnds), n)
  )
}
