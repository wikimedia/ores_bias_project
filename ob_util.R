library("data.table")
library("ggplot2")
library('forecast')
library("smooth")
library("lme4")
source("helper.R")
source("project_settings.R")
theme_set(theme_bw())

load.cutoffs <- function(){
    cutoffs  <- fread(file.path(data.dir,"ores_rcfilters_threshholds_updated.csv"))
    setnames(cutoffs,old=names(cutoffs), new=gsub('_','\\.',names(cutoffs)))
    cutoffs <- cutoffs[,deploy.dt := parse.date.iso(cutoffs$deploy.dt)]
                          
    ##handle special cases
    ## fawiki had a bug that we can ignroe
    cutoffs <- cutoffs[!((wiki.db == 'fawiki') & ( (deploy.dt == parse.date.iso("2017-12-09 11:19:00"))  | (deploy.dt == parse.date.iso("2017-12-11 18:56:00"))))]

                                        #frwiki and ruwiki had a bug that we can ignore
    cutoffs <- cutoffs[!((wiki.db=='frwiki') & (deploy.dt >= parse.date.iso("2017-11-09 14:35:00")))]

    cutoffs <- cutoffs[!((wiki.db=='ruwiki') & (deploy.dt >= parse.date.iso("2017-11-20 19:22:00")))]

                                        # kowiki's two commits have the same deploy time
    cutoffs <- cutoffs[!((wiki.db == 'kowiki') & (commit.dt == parse.date.iso("2019-03-04 11:26:06")))]

    cutoffs <- cutoffs[,next.deploy.dt := shift(deploy.dt,n=1,fill=parse.date.iso('2020-01-01 00:00:00'),type="lead"),by=.(wiki.db)]
    setkey(cutoffs,wiki.db,deploy.dt)
    return(cutoffs)
}

load.ts.ds <- function(){
    df <- fread(file.path(data.dir, "wiki_weeks.csv"),sep=',')
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))

    df <- df[,week:=parse.date.iso(week)]    

    cutoffs <- load.cutoffs()
    # need to do this merge on time as well. 
    df <- df[cutoffs,on=.(wiki.db=wiki.db,week>=deploy.dt,week<next.deploy.dt)]
    df[,weeks.from.deploy.dt := round((week - deploy.dt)/dweeks(1))]
    df[,treated := !all(is.na(weeks.from.deploy.dt)),by=.(wiki_db)]
    return(df)
}

load.rdd.ds <- function(){
    cutoffs  <- load.cutoffs()
    df <- fread(file.path(data.dir, "historically_scored_sample.csv"))
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))
    df <- df[,":="(date=parse.date.tz(date),event.timstamp=parse.date.iso(event.timestamp))]
    df <- df[cutoffs,on=.(wiki.db=wiki.db,date=deploy.dt)]
    return(df)
}


