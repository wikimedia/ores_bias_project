source("ob_util.R")
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

    # TODO add block of turkish wikipedia on 29 April 2017
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

        # since we have weekly data: 52 weeks in a year
        y <- ts(res$y,frequency=52)
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
