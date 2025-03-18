bes_colors <- function(type = c("primary", "accent", "logo")){
  type = match.arg(type)
  bes_data <- matrix(
    c(182, 204, 194,
      123, 169, 190,
      100, 177, 69,
      129, 141, 89,
      0,   122, 154,
      101, 58,  43,
      242, 118, 66,
      253, 175, 63,
      11,  122, 154
      ),
    ncol = 3,
    dimnames = list(c(paste0("primary", 1:6), paste0("accent", 1:2), "tableband"), NULL)
  )
  colorspace::RGB(R = bes_data)
}

# table 11, 122, 154
