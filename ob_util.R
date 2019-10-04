library("data.table")
library("ggplot2")
library('forecast')
library("smooth")
library("lme4")
source("helper.R")
source("project_settings.R")
theme_set(theme_bw())

load.cutoffs <- function(){
    cutoffs  <- fread(file.path(data.dir,"ores_rcfilters_thresholds.csv"))
    setnames(cutoffs,old=names(cutoffs), new=gsub('_','\\.',names(cutoffs)))
    cutoffs <- cutoffs[,deploy.dt := parse.date.iso(cutoffs$deploy.dt)]

    cutoffs <- cutoffs[order(wiki.db,deploy.dt)]

    ## add back the manual cutoffs
    manual.scores <-  fread(file.path(data.dir,"manually_checked_threshholds.csv"))
    setnames(manual.scores,old=names(manual.scores), new=gsub('_','\\.',names(manual.scores)))

    manual.scores <- manual.scores[,deploy.dt := as.POSIXct(manual.scores$deploy.dt,format="%Y-%m-%dT%TZ",tz='UTC')]
    manual.scores <- manual.scores[order(wiki.db,deploy.dt)]

    threshold.vars <- c("goodfaith.bad.max","goodfaith.bad.min","goodfaith.good.max","goodfaith.good.min","goodfaith.likelybad.max","goodfaith.likelybad.min","goodfaith.likelygood.max","goodfaith.likelygood.min","goodfaith.maybebad.max","goodfaith.maybebad.min","goodfaith.verylikelybad.max","goodfaith.verylikelybad.min")

    for(i in nrow(manual.scores)){
        wiki <- manual.scores[i,wiki.db]
        deploy  <- manual.scores[i,deploy.dt]

        for(var in threshold.var){
            score <- manual.scores[i,..var]
            cutoffs[(wiki.db == wiki) & (deploy.dt == deploy), paste0(var,'.value') := score]
        }
    }
        
    ##handle special cases
    ## fawiki", ruwiki, and frwiki had a bug that we can't ignore
#    cutoffs <- cutoffs[!((wiki.db == 'fawiki') & ( (deploy.dt == parse.date.iso("2017-12-09 11:19:00"))  | (deploy.dt == parse.date.iso("2017-04-11 18:56:00"))))]

                                        #frwiki and ruwiki had a bug that we can ignore
#    cutoffs <- cutoffs[!((wiki.db=='frwiki') & (deploy.dt >= parse.date.iso("2017-11-09 14:35:00")))]

#    cutoffs <- cutoffs[!((wiki.db=='ruwiki') & (deploy.dt >= parse.date.iso("2017-11-20 19:22:00")))]

                                        # kowiki's two commits have the same deploy time
    cutoffs <- cutoffs[!((wiki.db == 'kowiki') & (commit.dt == parse.date.iso("2019-03-04 11:26:06")))]

    cutoffs <- cutoffs[,next.deploy.dt := shift(deploy.dt,n=1,fill=parse.date.iso('2020-01-01 00:00:00'),type="lead"),by=.(wiki.db)]
    setkey(cutoffs,wiki.db,deploy.dt)
    return(cutoffs)
}

load.ts.ds <- function(){
    df <- fread(file.path(data.dir, "wiki_weeks.csv"),sep=',')
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))

    df <- df[,week:=parse.date.tz(week)]    

    cutoffs <- load.cutoffs()
    # need to do this merge on time as well. 

    df <- df[order(wiki.db,week)]

    df <- df[,og.week := week]

    cols.to.use <- c(
        "deploy.dt",
        "next.deploy.dt",
        "week",
        "og.week",
        "wiki.db",
        "has.ores",
        "has.rcfilters",
        "has.rcfilters.watchlist",
        "n.reverts",
        "user.week.var",
        "revert.hhi",
        "user.week.sd",
        "user.week.revert.cv",
        "mean.ttr",
        "sd.ttr",
        "geom.mean.ttr",
        "med.ttr",
        "undo.N.reverts",
        "rollback.N.reverts",
        "huggle.N.reverts",
        "twinkle.N.reverts",
        "otherTool.N.reverts",
        "undo.geom.mean.ttr",
        "rollback.geom.mean.ttr",
        "huggle.geom.mean.ttr",
        "twinkle.geom.mean.ttr",
        "otherTool.geom.mean.ttr",
        "LiveRC.geom.mean.ttr",
        "LiveRC.N.reverts",
        "fastbuttons.geom.mean.ttr",
        "fastbuttons.N.reverts", 
        "bot.N.reverts",
        "anonymous.geom.mean.ttr",
        "anonymous.N.reverts",
        "newcomer.geom.mean.ttr",
        "newcomer.N.reverts",
        "established.geom.mean.ttr",
        "established.N.reverts",
        "anonymous.undo.geom.mean.ttr",
        "anonymous.undo.N.reverts",
        "anonymous.rollback.geom.mean.ttr",
        "anonymous.rollback.N.reverts",
        "anonymous.huggle.geom.mean.ttr",
        "anonymous.huggle.N.reverts",
        "anonymous.twinkle.geom.mean.ttr",
        "anonymous.twinkle.N.reverts",
        "anonymous.otherTool.geom.mean.ttr",
        "anonymous.otherTool.N.reverts",
        "anonymous.fastbuttons.geom.mean.ttr",
        "anonymous.fastbuttons.N.reverts",
        "anonymous.LiveRC.geom.mean.ttr",
        "anonymous.LiveRC.N.reverts",
        "newcomer.undo.geom.mean.ttr",
        "newcomer.undo.N.reverts",
        "newcomer.rollback.geom.mean.ttr",
        "newcomer.rollback.N.reverts",
        "newcomer.huggle.geom.mean.ttr",
        "newcomer.huggle.N.reverts",
        "newcomer.twinkle.geom.mean.ttr",
        "newcomer.twinkle.N.reverts",
        "newcomer.otherTool.geom.mean.ttr",
        "newcomer.otherTool.N.reverts",
        "newcomer.fastbuttons.geom.mean.ttr",
        "newcomer.fastbuttons.N.reverts",
        "newcomer.LiveRC.geom.mean.ttr",
        "newcomer.LiveRC.N.reverts",
        "established.undo.geom.mean.ttr",
        "established.undo.N.reverts",
        "established.rollback.geom.mean.ttr",
        "established.rollback.N.reverts",
        "established.huggle.geom.mean.ttr",
        "established.huggle.N.reverts",
        "established.twinkle.geom.mean.ttr",
        "established.twinkle.N.reverts",
        "established.otherTool.geom.mean.ttr",
        "established.otherTool.N.reverts",
        "established.fastbuttons.geom.mean.ttr",
        "established.fastbuttons.N.reverts",
        "established.LiveRC.geom.mean.ttr",
        "established.LiveRC.N.reverts",
        "n.undos",
        "n.rollbacks")

    since.first.cutoff <- df[cutoffs,..cols.to.use,
                             on=.(wiki.db=wiki.db,week>=deploy.dt,week<next.deploy.dt),
                             ]

    first.cutoff <- cutoffs[cutoffs[,
                                    .(min.deploy.dt = min(deploy.dt)),
                                    by=.(wiki.db)],
                            on=.(wiki.db=wiki.db, deploy.dt = min.deploy.dt)]

    before.first.cutoff <- df[first.cutoff,
                              ..cols.to.use,
                              on=.(wiki.db=wiki.db, week < deploy.dt)]

    before.first.cutoff[,':='(has.rcfilters.watchlist = FALSE,
                              has.rcfilters = FALSE,
                              has.ores = FALSE)]

    df <- rbindlist(list(since.first.cutoff,before.first.cutoff))
    # add back the edits before the first deploy.dt and set the has.* variables to FALSE
    df[,week:=og.week]
    df[,og.week:=NULL]
    df[,weeks.from.deploy.dt := round(difftime(week,deploy.dt,units='weeks'))]
    wikis.to.exclude <- c("mediawikiwiki","test2wiki","testwiki")
    df <- df[ !(wiki.db %in% wikis.to.exclude)]
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
