library("data.table")
library("ggplot2")
library('forecast')
library("smooth")
library("lme4")
source("helper.R")
source("project_settings.R")
theme_set(theme_bw())

load.cutoffs <- function(){
    cutoffs  <- fread(file.path(data.dir,"ores_rcfilters_cutoffs.csv"))
    setnames(cutoffs,old=names(cutoffs), new=gsub('_','\\.',names(cutoffs)))
    cutoffs <- cutoffs[,deploy.dt := parse.date.iso(cutoffs$deploy.dt)]

    cutoffs <- cutoffs[order(wiki.db,deploy.dt)]
    ##handle special cases
    ## fawiki, ruwiki, and frwiki had a bug that we can't ignore
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

shape.model.data <- function(wiki.model.data, dv){

    wiki.model.data <- wiki.model.data[order(week,decreasing=FALSE)]
    wiki.model.data[is.na(wiki.model.data[[dv]]), dv] <- 0L
    if(all(wiki.model.data[[dv]] == 0L)){
        wiki <- unique(wiki.model.data$wiki)
        print(paste(wiki,"is all 0 for this measure"))
        y <- rep(0L,length(wiki.model.data[[dv]]))
    }    else{
        wiki.model.data <- wiki.model.data[,(dv) := scale(wiki.model.data[[dv]],center=F,scale=T)]
    
        y <- wiki.model.data[[dv]]
    }

    xreg <- wiki.model.data[,.(has.ores = as.numeric(has.ores),
                               has.rcfilters = as.numeric(has.rcfilters),
                               has.rcfilters.watchlist = as.numeric(has.rcfilters.watchlist))]
    
    if(all(xreg[,has.ores] == xreg[,has.rcfilters])){
        xreg[,has.ores := NULL]
    } else  if(all(xreg[,has.ores] == xreg[,has.rcfilters.watchlist])){
        xreg[,has.ores := NULL]
    }

    if(all(xreg[,has.rcfilters]==xreg[,has.rcfilters.watchlist])){
        xreg[,has.rcfilters := NULL]
    }

    xreg <- xreg[,c(!apply(xreg,2,function(x) all(x==0) | all(x==1))),with=F]
    
    xreg <- as.matrix(xreg,colnames = names(xreg))
    
    return(list(y=y,xreg=xreg,wiki.model.data=wiki.model.data))
}


fit.models <- function(model.data, dv){
    arima.models <- list()
    for(wiki in unique(model.data$wiki.db)){
        print(wiki)
        wiki.model.data <- model.data[wiki.db == wiki]

        res <- shape.model.data(wiki.model.data, dv)
        y <- res$y
        xreg  <- res$xreg
        wiki.model.data <- res$wiki.model.data
        # for the final analysis we shouldn't use stepwise selection
        #        fit.model <- auto.arima(y = model.data[[dv]],xreg=as.numeric(model.data$has.ores),ic='bic',parallel=T,stepwise=F,approximation=F,num.cores=20)

        ## we need to detect how many cutoffs are in play. 
        ## name the cutoffs, and then choose the ones that aren't identical with the priority:
        ## watchlist, rcfilters, has.ores

    
#        fit.model <- auto.arima(y,xreg=xreg, ic='bic', parallel=T,stepwise=F,approximation=F)
        fit.model <- auto.arima(y,xreg=xreg, ic='bic')
        arima.models[[wiki]] <- fit.model
        
        model.data[wiki.db==wiki, (paste0('pred.',dv)) := fit.model$fitted]
        model.data[wiki.db==wiki, (paste0('scaled.',dv)) :=  wiki.model.data[[dv]]]

    }
    return( list(model.data = model.data, arima.models=arima.models))
}

get.est.interval <- function(model){
    if('has.rcfilters.watchlist' %in% names(coef(model))){
        
        est <- coef(model)['has.rcfilters.watchlist']
        var <- vcov(model)['has.rcfilters.watchlist','has.rcfilters.watchlist']
        se <- sqrt(var)
        return(list(lower = est - 1.96*se, est = est, upper = est + 1.96*se))
    }
}

plot.model.fit <- function(model.data, dv){
    plot.data <- melt(model.data,measure.vars = c(paste0('pred.',dv),paste0('scaled.',dv)))
    p <- ggplot(plot.data,
                aes(x=week,
                    y=value,
                    group=variable,
                    color=variable)) +
        geom_line(alpha=0.5) +
        geom_vline(aes(xintercept=deploy.dt),linetype=2) +
        facet_wrap(.~wiki.db,scale='free',ncol=1)

    output.dir = 'saved.plots'
    if(!dir.exists(output.dir)){
        dir.create(output.dir)
    }
    ggsave(paste0("saved.plots/fitted.",dv,'.pdf'), p, 'pdf', height=40)
    return(p)
}

plot.model.coefficients <- function(arima.models, dv){
    plot.data <- rbindlist(lapply(arima.models,get.est.interval))
    plot.data$wiki.db = names(arima.models)
    plot.data <- plot.data[order(-est)]
    plot.data[,wiki.db := gsub("wiki","",wiki.db)]

    plot.data[,wiki.db := factor(wiki.db,levels=plot.data[order(-est)]$wiki.db)]
    plot.data[lower > 0, sig := 'gtzero']
    plot.data[upper < 0, sig := 'ltzero']
    plot.data[ (lower < 0) & (upper > 0), sig := 'nonsig']
    p <- ggplot(plot.data, aes(x=wiki.db,
                               y=est,
                               ymax=upper,
                               ymin=lower,
                               color=sig)) +
        geom_pointrange() +
        ggtitle(paste0("Model estimates for ", dv))

    ggsave(paste0("saved.plots/coefficients.",dv,'.pdf'))
    return(p)
}

model.timeseries <- function(model.data,dv){
    res <- fit.models(model.data,dv)
    model.data <- res$model.data
    arima.models <- res$arima.models
    p1 <- plot.model.fit(model.data, dv)
    p2 <- plot.model.coefficients(arima.models,dv)
    return(list(plot=p2,models=arima.models,dv=dv))
}
