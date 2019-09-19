parse.date.tz <- function(s) as.POSIXct(s, format="%Y-%m-%dT%T.000Z",tz='UTC')
parse.date.iso <- function(s) as.POSIXct(s, format="%Y-%m-%d %T",tz='UTC')


