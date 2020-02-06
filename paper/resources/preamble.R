library(ggplot2)
theme_set(theme_minimal())
library(data.table)
library(xtable)
#set xtable options
options(xtable.floating = FALSE)
options(xtable.timestamp = '')
options(xtable.include.rownames=FALSE)

bold <- function(x) {paste('{\\textbf{',x,'}}', sep ='')}
gray <- function(x) {paste('{\\textcolor{gray}{',x,'}}', sep ='')}
wrapify <- function (x) {paste("{", x, "}", sep="")}

# load("knitr_data.RData"); now broken up into small files so we'll bring 'em all in together

f <- function (x) {formatC(x, format="d", big.mark=',')}

format.percent <- function(x) {paste(f(x*100),"\\%",sep='')}

format.day.ordinal <- function(x) {
    day <- format(x,format="%d")
    daylast <- substr(day,nchar(day),nchar(day))
    dayfirst <- substr(day,1,1)
    if(dayfirst == '0')
        day = daylast

    if( daylast == "1")
        day <- paste0(day,"st")
    else if(daylast == "2")
        day <- paste0(day,"nd")
    else if (daylast == "3")
        day <- paste0(day,"rd")
    else
        day <- paste0(day,"th")
        
    return(day)
}

format.month <- function(x){
    return( format(x,format='%B %Y'))
}

format.date <- function(x) {
    return(paste(format(x,format = '%B'),format.day.ordinal(x),format(x,format='%Y'),sep=' '))
}

format.pvalue <- function (x, digits=3) {
    threshold <- 1*10^(-1*digits)
    x <- round(x, digits)
    if (x < threshold) {
        return(paste("p<", threshold, sep=""))
    } else {
        return(paste("p=", x, sep=""))
    }
}

sparkplot <- function(samples){
  # place lines (or maybe shading?) at the mean and credible interval
  p <- qplot(samples, geom="density") + ggtitle("") + xlab("") + ylab("") + scale_y_continuous(breaks=c()) + theme_minimal() + scale_x_continuous(breaks=c())
  plot.data <- as.data.table(ggplot_build(p)[1]$data)
  ci.95 <- quantile(samples,c(0.025, 0.975))
  ci.region <- plot.data[(x>=ci.95[1]) & (x<=ci.95[2])]
  #  p <- p + geom_area(data=ci.region, aes(x=x,y=y), fill='grey',alpha=0.6)
  p <- p + geom_vline(xintercept=ci.95[1],color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=ci.95[2], color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=mean(samples), color='blue', linetype='dashed',size=3)
  p <- p + geom_vline(xintercept=0, color='black',size=3)
  return(p)
}

r <- readRDS("resources/remembr_hyak.RDS")
attach(r)
r2 <- readRDS("resources/notebook_remember.RDS")
attach(r2)
