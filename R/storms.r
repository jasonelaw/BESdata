#' Detect Storms According to ASM CSO Program Business Rules
#'
#' The storm detection algorithm is documented in OneNote, a pdf document, and
#' a memo. The algorithm relies on start points that occur when it is currently
#' raining, a forward looking rolling sum is over the threshold, and the previous
#' time points within the IET (i.e., (i-iet+1):(i-1) time points where the current
#' point is i. A storm continues if the previous point is a storm, a backward sum
#' is not
#' @param x a vector of rain amounts
#' @param time a vector of time stamps, used to check order if present
#' @param threshold a scalar representing the threshold for forward and backward sums
#' @param iet a scalar representing the minimum inter-event time duration
#' @export
cso_storm <- function(x, time, threshold = 0.1, iet = 10){
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
  At <- logical(n)                               # Active event - initialize to FALSE
  Ft <- RcppRoll::roll_suml(x, iet, fill  = 0) >= threshold # forward sum over threshold
  Bt <- RcppRoll::roll_sumr(x, iet, fill = 0) >= threshold  # backward sum over threshold
  Ct    <- x  >  0                               # currently raining
  Ft.Ct  <- Ft & Ct #Ft * Ct                     # starting point
  Ft_nlag1Bt <- Ft | !lag1(Bt)                   # stopping point

  for (i in (iet+2):n){
    # iet check index
    j <- (i-iet+1):(i-1)
    # Is t-1 an event and is t not a stopping point
    T1 <- At[i-1] && Ft_nlag1Bt[i]
    # OR - It's a start point and no event in iet
    T2 <- Ft.Ct[i] && all(!At[j])
    At[i] <- T1 || T2
  }
  At
}

#' @export
calcStats <- function(x, storm){
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
  ret <- as_tibble(t(ret))
  ret$which.max <- ret$which.max + start - 1
  ret$max   <- x[ret$which.max]
  vctrs::vec_cbind(vctrs::new_data_frame(storm), ret)
}

#' Accepts a boolean x, and a size argument. Looks for runs of true in boolean
#' and
decluster <- function(x, size){
  ix   <- which(x)
  nix <- length(ix)
  idix <- which(diff(ix) > size)
  iend <- ix[c(idix, nix)]
  isrt <- ix[c(1L, idix + 1L)]
  ret <- new_bounds(start = isrt, end = iend, size = vctrs::vec_size(x))
  bounds_to_bool(ret)
}

#' @export
pot <- function(x, threshold, min.diff){
  pot <- x > threshold
  decluster(pot, min.diff)
}

# Helpers ----------------------------------------------------------------------

lag1 <- function(x) c(0, x[1:(length(x)-1)])

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
