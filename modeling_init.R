library(logistf)
library(ggplot2)
library(gridExtra)
library(grid)
library(data.table)
source("helper.R")
source("ob_util.R")
source("RemembR/R/RemembeR.R")
library(rstanarm)
library(parallel)
mcaffinity(1:detectCores()) ## required and explained below 
options(mc.cores = parallel::detectCores())
theme_set(theme_bw())
df <- load.rdd.ds()
df <- add.rescored.revisions(df)
df <- transform.threshhold.variables(df)

rcfilters.dates <- df[(has.ores==T) & (has.rcfilters==T) & (has.rcfilters.watchlist==T), .(date=min(date)),by=wiki.db]
df <- df[,":="(post.cutoff.fact = as.factor(pre.cutoff == FALSE),
               wiki.db = as.factor(wiki.db),
               revision.is.identity.reverted.bin=revision.is.identity.reverted=="TRUE")]
df.cutoff <- df[rcfilters.dates, on=.(wiki.db,date)]
df.pre <- df.cutoff[pre.cutoff==T]
df.post <- df.cutoff[pre.cutoff==F]

gen.synthetic.data <- function(wiki, df.cutoff){

    syn.cutoffs <- first(df.cutoff[wiki.db==wiki,.(damaging.maybebad.min,
                                                       damaging.likelybad.min,
                                                       damaging.verylikelybad.min
                                                       )])

    syndata <- data.table(prob.damaging = 0:1000/1000)

    syndata[,":="(pred.damaging.likelybad=as.factor(prob.damaging >= syn.cutoffs$damaging.likelybad.min),
                  pred.damaging.verylikelybad=as.factor(prob.damaging >= syn.cutoffs$damaging.verylikelybad.min),
                  pred.damaging.maybebad=as.factor(prob.damaging >= syn.cutoffs$damaging.maybebad.min),
                  has.rcfilters.watchlist=FALSE,
                  wiki.db=wiki
                  )]

    syndata2 <- copy(syndata)

    syndata2[,has.rcfilters.watchlist:=TRUE]

    syndata <- rbindlist(list(syndata,syndata2))

    syndata <- syndata[,":="(has.rcfilters.watchlist = as.factor(has.rcfilters.watchlist),
                             post.cutoff = has.rcfilters.watchlist,
                             post.cutoff.fact = as.factor(has.rcfilters.watchlist))]

    return(syndata)
}
