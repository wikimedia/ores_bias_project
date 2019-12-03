library(data.table)
library(ggplot2)
library(forecast)
library(smooth)
library(lme4)
source("helper.R")
source("project_settings.R")
theme_set(theme_bw())

load.cutoffs <- function(){
#    cutoffs  <- fread(file.path(data.dir,"ores_rcfilters_thresholds.csv"))
    cutoffs <-  fread(file.path(data.dir,"ores_rcfilters_thresholds_withdefaults.csv"))
    setnames(cutoffs,old=names(cutoffs), new=gsub('_','\\.',names(cutoffs)))
    cutoffs <- cutoffs[,deploy.dt := parse.date.iso(cutoffs$deploy.dt)]

    cutoffs <- cutoffs[order(wiki.db,deploy.dt)]

    ## add back the manual cutoffs
    manual.scores <-  fread(file.path(data.dir,"manually_checked_thresholds.csv"))
    setnames(manual.scores,old=names(manual.scores), new=gsub('_','\\.',names(manual.scores)))

    manual.scores <- manual.scores[,deploy.dt := as.POSIXct(manual.scores$deploy.dt,format="%Y-%m-%dT%TZ",tz='UTC')]
    manual.scores <- manual.scores[order(wiki.db,deploy.dt)]

    threshold.vars <- grepl("(damaging.*value)|(goodfaith.*value)",names(cutoffs))
    threshold.vars  <- names(cutoffs)[threshold.vars]

    #threshold.vars <- gsub(".value","",threshold.vars)

    ## for(var in threshold.vars){
    ##     cutoffs[,paste0(var,'.value'):=as.numeric(0.0)]
    ## }

    for(i in 1:nrow(manual.scores)){
        wiki <- manual.scores[i,wiki.db]
        deploy  <- manual.scores[i,deploy.dt]

        for(var in threshold.vars){
            man.var <- gsub(".value","",var)
            cutoffs[,(var) := as.numeric(cutoffs[[var]])]
            score <- manual.scores[i,..man.var]
            cutoffs[(wiki.db == wiki) & (deploy.dt == deploy), (var) := score]
        }
    }
        
    # in the 2 period design we don't need to do this
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

load.ts.ds <- function(cutoffs = NULL){
    df <- fread(file.path(data.dir, "wiki_weeks.csv"),sep=',')
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))

    df <- df[,week:=as.POSIXct(week, formay="%Y-%m-%d", tz='UTC')]
#    df <- df[,week:=parse.date.iso(week)]    

    if(is.null(cutoffs)){
        cutoffs <- load.cutoffs()
    }

    # need to do this merge on time as well. 

    df <- df[order(wiki.db,week)]

    df <- df[,og.week := week]

    cols.to.use <- c(
        "deploy.dt",
        "next.deploy.dt",
        "week",
        "n.reverts",
        "og.week",
        "wiki.db",
        "has.ores",
        "has.rcfilters",
        "has.rcfilters.watchlist",
        "n.reverts",
        "anonymous.N.reverts",
        "newcomer.N.reverts", 
        "anonymous.N.edits",
        "anonymous.P.reverted",
        "anonymous.N.damage.reverted",
        "newcomer.N.edits",
        "newcomer.P.reverted",
        "newcomer.N.damage.reverted",
        "established.N.edits",
        "established.P.reverted",
        "established.N.damage.reverted"
        )

        ## "user.week.var",
        ## "revert.hhi",
        ## "user.week.sd",
        ## "user.week.revert.cv",
        ## "mean.ttr",
        ## "sd.ttr",
        ## "geom.mean.ttr",
        ## "med.ttr",
        ## "undo.N.reverts",
        ## "rollback.N.reverts",
        ## "huggle.N.reverts",
        ## "twinkle.N.reverts",
        ## "otherTool.N.reverts",
        ## "undo.geom.mean.ttr",
        ## "rollback.geom.mean.ttr",
        ## "huggle.geom.mean.ttr",
        ## "twinkle.geom.mean.ttr",
        ## "otherTool.geom.mean.ttr",
        ## "LiveRC.geom.mean.ttr",
        ## "LiveRC.N.reverts",
        ## "fastbuttons.geom.mean.ttr",
        ## "fastbuttons.N.reverts", 
        ## "bot.N.reverts",
        ## "anonymous.geom.mean.ttr",
        ## "anonymous.N.reverts",
        ## "newcomer.geom.mean.ttr",
        ## "newcomer.N.reverts",
        ## "established.geom.mean.ttr",
        ## "established.N.reverts",
        ## "anonymous.undo.geom.mean.ttr",
        ## "anonymous.undo.N.reverts",
        ## "anonymous.rollback.geom.mean.ttr",
        ## "anonymous.rollback.N.reverts",
        ## "anonymous.huggle.geom.mean.ttr",
        ## "anonymous.huggle.N.reverts",
        ## "anonymous.twinkle.geom.mean.ttr",
        ## "anonymous.twinkle.N.reverts",
        ## "anonymous.otherTool.geom.mean.ttr",
        ## "anonymous.otherTool.N.reverts",
        ## "anonymous.fastbuttons.geom.mean.ttr",
        ## "anonymous.fastbuttons.N.reverts",
        ## "anonymous.LiveRC.geom.mean.ttr",
        ## "anonymous.LiveRC.N.reverts",
        ## "newcomer.undo.geom.mean.ttr",
        ## "newcomer.undo.N.reverts",
        ## "newcomer.rollback.geom.mean.ttr",
        ## "newcomer.rollback.N.reverts",
        ## "newcomer.huggle.geom.mean.ttr",
        ## "newcomer.huggle.N.reverts",
        ## "newcomer.twinkle.geom.mean.ttr",
        ## "newcomer.twinkle.N.reverts",
        ## "newcomer.otherTool.geom.mean.ttr",
        ## "newcomer.otherTool.N.reverts",
        ## "newcomer.fastbuttons.geom.mean.ttr",
        ## "newcomer.fastbuttons.N.reverts",
        ## "newcomer.LiveRC.geom.mean.ttr",
        ## "newcomer.LiveRC.N.reverts",
        ## "established.undo.geom.mean.ttr",
        ## "established.undo.N.reverts",
        ## "established.rollback.geom.mean.ttr",
        ## "established.rollback.N.reverts",
        ## "established.huggle.geom.mean.ttr",
        ## "established.huggle.N.reverts",
        ## "established.twinkle.geom.mean.ttr",
        ## "established.twinkle.N.reverts",
        ## "established.otherTool.geom.mean.ttr",
        ## "established.otherTool.N.reverts",
        ## "established.fastbuttons.geom.mean.ttr",
        ## "established.fastbuttons.N.reverts",
        ## "established.LiveRC.geom.mean.ttr",
        ## "established.LiveRC.N.reverts",
        ## "n.undos",
        ## "n.rollbacks")

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
    df <- df[, ":="(month = factor(month(week)), n.week = factor(week(week)))]
    df <- df[, time.days := week - parse.date.iso("2000-01-01 00:00:00")]
    return(df)
}

load.rdd.ds <- function(filename="historically_scored_sample.csv", cutoffs=NULL){
    if(is.null(cutoffs)){
        cutoffs <- load.cutoffs()
    }

    df <- fread(file.path(data.dir, filename))
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))

    df <- df[,":="(date=parse.date.iso(date),event.timstamp=parse.date.tz(event.timestamp))]
    
    df <- df[cutoffs,':='(has.ores = i.has.ores,
                          has.rcfilters = i.has.rcfilters,
                          has.rcfilters.watchlist=i.has.rcfilters.watchlist,
                          damaging.likelybad.min = as.numeric(i.damaging.likelybad.min.value),
                          damaging.verylikelybad.min = as.numeric(i.damaging.verylikelybad.min.value),
                          damaging.maybebad.min = as.numeric(i.damaging.maybebad.min.value),
                          damaging.likelygood.max = as.numeric(i.damaging.likelygood.max.value),
                          deploy.dt = i.deploy.dt,
                          pre.cutoff = toupper(pre.cutoff)=='TRUE'
                          ),
             on=.(wiki.db=wiki.db,date=deploy.dt)]
    return(df)
}

add.rescored.revisions <- function(df){
    rescored <- load.rdd.ds("rescored_revisions.csv")
    rescored[,revision.id := as.integer(revision.id)]
    rescored <- rescored[,.(wiki.db, revision.id, prob.damaging, revscoring.error)]
    merged <- merge(df,rescored,by=c("wiki.db","revision.id"),all.x=TRUE, suffixes=c('','.y'))
    merged[!is.na(prob.damaging.y),":="(prob.damaging = prob.damaging.y,
                                          revscoring.error = revscoring.error.y)]
    merged$prob.damaging.y = NULL
    merged$revscoring.error.y = NULL
    return(merged)
}

transform.threshold.variables <- function(df){
    df[,":="(pred.damaging.maybebad = as.factor(prob.damaging > damaging.maybebad.min),
             d.maybebad.threshold = damaging.maybebad.min - prob.damaging,
             pred.damaging.likelybad = as.factor(prob.damaging > damaging.likelybad.min),
             d.likelybad.threshold = damaging.likelybad.min - prob.damaging,
             pred.damaging.verylikelybad = as.factor(prob.damaging > damaging.verylikelybad.min),
             d.verylikelybad.threshold = damaging.verylikelybad.min - prob.damaging,
             pred.damaging.likelygood = as.factor(prob.damaging < damaging.likelygood.max),
             d.likelygood.threshold = damaging.likelygood.max - prob.damaging,
             revision.is.identity.reverted = as.factor(toupper(revision.is.identity.reverted))
             )
       ]


    df[,":="(d.abs.nearest.threshold = pmin(abs(d.maybebad.threshold),
                                       abs(d.likelybad.threshold),
                                       abs(d.verylikelybad.threshold),na.rm=T))]
             

    df[,":="(nearest.threshold = ifelse(d.abs.nearest.threshold==abs(d.maybebad.threshold),"maybebad",
                                 ifelse(d.abs.nearest.threshold==abs(d.likelybad.threshold),"likelybad",
                                 ifelse(d.abs.nearest.threshold==abs(d.verylikelybad.threshold),"verylikelybad",NA))))]

    df[,":="(d.nearest.threshold = ifelse(nearest.threshold == 'maybebad', d.maybebad.threshold,
                                   ifelse(nearest.threshold == 'likelybad', d.likelybad.threshold,
                                   ifelse(nearest.threshold == 'verylikelybad',d.verylikelybad.threshold,NA))))]

    df[,":="(gt.nearest.threshold = ifelse(nearest.threshold == 'maybebad', pred.damaging.maybebad,
                                    ifelse(nearest.threshold == 'likelybad', pred.damaging.likelybad,
                                    ifelse(nearest.threshold == 'verylikelybad',pred.damaging.verylikelybad,NA))))]

    return(df)
}


validate.rdd.ds <- function(df){
# check if we are missing scores for a large proportion of edits for given revisions
    print(df[,.(prop.missing=mean(is.na(prob.damaging))),by=.(wiki.db,commit,date)][prop.missing>0.05])
    error.records <- df[,.(prop.missing=mean(is.na(prob.damaging))),by=.(wiki.db,commit,date)][prop.missing>0.99]
    error.data <- df[error.records,on=.(wiki.db,commit)]
    ## we have problems with it
    error.data <- error.data[,":="(i.date=NULL,prop.missing=NULL,prob.damaging=NULL,revscoring.error=NULL)]
    setnames(error.data,old=names(error.data),new=gsub('\\.','_',names(error.data)))
    fwrite(error.data, file.path(data.dir, 'revisions_to_rescore.csv'))
}


