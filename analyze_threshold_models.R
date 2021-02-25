library(xtable)
source("modeling_init.R")
source("ob_util.R")

fit.placebo.tests <- function(model.call, p, n.checks, filter= function(x) x, base.name=''){
    ## generate fake cutoffs
    cutoff.deltas <- seq(2*p, n.checks*p, 2*p)
    cutoff.deltas <- c(-1 * cutoff.deltas, cutoff.deltas)

    models <- NULL
    for(placebo.delta in cutoff.deltas){
        cutoffs <- load.cutoffs()
        threshold.vars <- names(cutoffs)[grepl("(damaging.*value)|(goodfaith.*value)",names(cutoffs))]
        cutoffs[,(threshold.vars) := lapply(.SD, function(col) col + placebo.delta), .SDcols=threshold.vars]
        placebo.df <- build.rdd.dataset(cutoffs=cutoffs)
        placebo.df <- filter(placebo.df)
        fit.model <- model.call(data=data.frame(placebo.df))
        fit.model$loo <- loo(fit.model)
        fit.model$waic  <- waic(fit.model)
        name  <- paste0(base.name,'.',placebo.delta)
        if(is.null(models)){
            models  <- stanreg_list(fit.model, model_names=c(name))
        } else {
            models  <- stanreg_list(models, fit.model, model_names=c("",name))
        }
        print(models)
    }
    return(models)
}

plot.general.model <- function(exemplar.wiki='enwiki', model=NULL, model.file=NULL, model.name = "", plot.data = TRUE, n.bins=20, prob=0.02, newdata=NULL, placebo.shift = 0){

    model <- readRDS(model.file)
    if(plot.data == TRUE){
        p <- plot.bins(exemplar.wiki=exemplar.wiki, model, n.bins, prob)
    } else {
        p <- ggplot()
    }

    if(is.null(newdata)){
        df <- as.data.table(model$data)
    } else {
        df <- newdata
    }
    if(exemplar.wiki == "all"){
        df <- gen.syndata.with.predictions('enwiki', df, model, prob=prob, transform=TRUE, placebo.shift=placebo.shift)
    }
    ## } else {
    ##     df <- add.predictions.confint(model, df, transform=TRUE)
    ## }    

    p <- plot.threshold.cutoffs(df, paste0(exemplar.wiki,'_',model.name), partial.plot=p)
    filename <-  paste0('overall_model_',exemplar.wiki,'_',model.name,'_','_cutoffs.pdf')
    cairo_pdf(filename)
    print(p)
    dev.off()
    return(p)
}

gen.bins.data <- function(exemplar.wiki = NULL, model, n.bins, prob, partial.plot = NULL, weighted=TRUE){
    df <- as.data.table(model$data)
    if(!is.null(exemplar.wiki) && (exemplar.wiki != "all")){
        df <- df[wiki.db == exemplar.wiki]
    }

    threshold.set <- list()
    
    for(threshold in unique(df$nearest.threshold)){
                                        # need to fix the bins so the line up with the threshold
        bins <- c(seq(from=-prob, to=0, by= prob/n.bins*2),seq(from=prob/n.bins*2, to=prob, by= prob/n.bins*2))
        binned <- data.table(bin=bins, threshold=threshold)
        binned <- binned[,next.bin := shift(bin,1,1,type='lead')]
        binned <- binned[,bin.mid := bin + (next.bin-bin)/2]
        binned <- binned[,bin.1 := bin]
        binned <- binned[bin!=max(bin)]
        threshold.set[[threshold]]  <-  df[binned, on=.(nearest.threshold=threshold, d.nearest.threshold>bin, d.nearest.threshold<=next.bin),nomatch=FALSE]
    }

    df <- rbindlist(threshold.set)

    outcome <- all.vars(model$formula)[1] 

                                        # the weights from the stratified sample don't sum to one since they are between-wiki weights
    df[["outcome"]] = as.numeric(df[[outcome]]==TRUE)
    df <- df[!is.na(outcome)]

    linkf <- family(model)$linkfun
    linkinvf <- family(model)$linkinv

    ## if(weighted == TRUE){
    ##     # create new weights by scaling the old weights and make correct per-wiki aggregations
    ##     df <- df[,":="(weight = weight/sum(weight),,by=.(wiki.db,nearest.threshold,gt.nearest.threshold)]
    ##     df <- df[,":="(prob.outcome = linkinvf(sum(wiki.logodds*wiki.weight)/sum(wiki.weight)), logodds=sum(wiki.logodds*wiki.weight)/sum(wiki.weight)),by=.(nearest.threshold,bin.mid)]
    ##                                     # rescale the weights so they sum to 1
    ##     df <- df[,":="(sd.weight = weight/sum(weight)), by=.(nearest.threshold,bin.mid)]
    ##     df <- df[, ":=" (sd.outcome=sqrt( sum(sd.weight * (outcome - prob.outcome)^2)),
    ##                      N=.N,
    ##                      sum.weight=sum(sd.weight)),by=.(nearest.threshold, bin.mid)]
    ## } else {
    ##     df <- df[,":="(N=.N, prob.outcome=mean(outcome), bin=first(bin.1)) ,by=.(nearest.threshold,bin.mid)]
    ##     df <- df[, sd.outcome := sqrt(sum((outcome - prob.outcome)^2)/N),by=.(nearest.threshold, bin.mid)]
    ## }

    if(weighted == TRUE){
#        df <- df[,":="(weight = sum(weight)),by=.(wiki.db,nearest.threshold,gt.nearest.threshold)]
#        df <- df[,":="(weight = weight/sum(weight)),by=.(nearest.threshold,gt.nearest.threshold,bin.mid)]
        df <- df[,":="(N=.N, prob.outcome=sum(outcome*weight)/sum(weight), bin=first(bin.1)) ,by=.(nearest.threshold,bin.mid)]
        df <- df[, ":=" (sd.outcome=sqrt( sum(weight * (outcome - prob.outcome)^2)),
                         N=.N,
                         sum.weight=sum(weight)),by=.(nearest.threshold, bin.mid)]
        df <- df[, se.outcome := sd.outcome/sqrt(sum.weight),by=.(nearest.threshold, bin.mid)]
        df <- df[, se.bern := (prob.outcome*(1-prob.outcome))/sqrt(sum.weight)]
    } else {
        df <- df[,":="(N=.N, prob.outcome=mean(outcome), bin=first(bin.1)) ,by=.(nearest.threshold,bin.mid)]
        df <- df[, sd.outcome := sqrt(sum((outcome - prob.outcome)^2)),by=.(nearest.threshold, bin.mid)]
        df <- df[, se.outcome := sd.outcome/sqrt(N),by=.(nearest.threshold, bin.mid)]
        df <- df[, se.bern := (prob.outcome*(1-prob.outcome))/sqrt(N)]
    }

    data.plot <- df[,lapply(.SD, first), by=.(nearest.threshold,bin.mid),.SDcols=c('N','prob.outcome','sd.outcome','se.outcome','se.bern')]
    data.plot <- data.plot[, nearest.threshold := factor(nearest.threshold, levels=c('maybebad','likelybad','verylikelybad'))]

    return(data.plot)
}



plot.smoother <- function(exemplar.wiki, model, n.bins, prob, partial.plot = NULL){
    if (is.null(partial.plot)){
        partial.plot <- ggplot()
    }

    df <- as.data.table(model$data)
    df <- df[wiki.db == exemplar.wiki]<

    outcome <- all.vars(model$formula)[1] 
    df[,outcome:= as.numeric(.SD[[outcome]]==TRUE)]

    p <- partial.plot + geom_smooth(data=df, aes(x=d.nearest.threshold, y=outcome))
    
    return(p)

}

add.predictions.confint <- function(model, df, transform){
    if(is.null(df)){
        df = model$data
    }

    ppd <- posterior_predict(model, newdata=df)
    df$pred <- apply(ppd,2,mean)

    ppd.interval <- predictive_interval(model)
                                        # the posterior predictive interval isn't what we want for interpreting statistical significance of differences
    linpred.res <- posterior_linpred(model, transform=transform)
    linpred.interval <- t(apply(linpred.res, 2, function(r) quantile(r, c(0.025,0.5, 0.975))))

    df$pred.lower <- ppd.interval[,1] 
    df$pred.upper <- ppd.interval[,2] 

    df$linpred.lower <- linpred.interval[,1] 
    df$linpred <- linpred.interval[,2]

    df$linpred.upper <- linpred.interval[,3]
    return(as.data.frame(df))
}

## remove.intercepts removes the wiki-level intercepts
gen.syndata.with.predictions <- function(wiki, df.cutoff, model, prob=0.2, transform=FALSE, placebo.shift=0, n=100){

    dv <- all.vars(model$formula)[[1]]

    if(wiki == "all"){
        syndata <- rbindlist(lapply(unique(df.cutoff$wiki.db),
                                    function(wiki) gen.synthetic.data(wiki, df.cutoff, dv, placebo.shift=placebo.shift,n=n)))
    } else {
        syndata <- gen.synthetic.data(wiki, df.cutoff, dv, placebo.shift=placebo.shift, n=n)
    }
        

    # drop NA columns that we aren't using right now
    ## syndata$d.likelygood.threshold = NULL
    ## syndata$pred.damaging.likelygood = NULL
    ## syndata$damaging.likelygood.max = NULL

#    syn.ppd <- posterior_predict(model, newdata=syndata)
#    syndata$pred <- apply(syn.ppd,2,mean)

#    ppd.interval <- predictive_interval(model, newdata=syndata)
                                        # the posterior predictive interval isn't what we want for interpreting statistical significance of differences
    linpred.res <- posterior_linpred(model, newdata=syndata, transform=transform)

    ## What about the idea of generating a synthetic data trendline that's an average of synthetic lines for each wiki?
    ## Weighted by the total weight in the means?
    ## Seems like that would work. Hopefully it would be smooth!

    linpred.interval <- t(apply(linpred.res, 2, function(r) quantile(r, c(0.025,0.5, 0.975))))

    #syndata$pred.lower <- ppd.interval[,1] 
    #syndata$pred.upper <- ppd.interval[,2] 

    syndata$linpred.lower <- linpred.interval[,1] 
    syndata$linpred <- linpred.interval[,2]

    syndata$linpred.upper <- linpred.interval[,3]

    #syndata$pred <- apply(syn.ppd,2,mean)

    syndata$nearest.threshold <- factor(syndata$nearest.threshold, levels = c("maybebad","likelybad","verylikelybad"))


    syndata[["outcome"]] <- as.numeric(dv==TRUE)
    syndata <- syndata[!is.na(outcome)]

    if(wiki=='all'){
        #x.bins.mid <- syndata$prob.damaging
        # normalize weights within wikis
        weights <- df.cutoff[,.(weight = sum(weight)),by=.(wiki.db,nearest.threshold,gt.nearest.threshold)]

        syndata.weighted <- syndata[weights, on=.(wiki.db, nearest.threshold,gt.nearest.threshold)]
        # normalize weights at each x
        syndata.weighted <- syndata.weighted[,weight := weight / sum(weight), by=.(nearest.threshold,d.nearest.threshold)]

        ## i don't actually want to take the average of the upper and lowers.
        ## i want to 
        syndata.weighted <- syndata.weighted[, .(linpred.upper = sum(linpred.upper * weight),
                                                 linpred.lower = sum(linpred.lower * weight),
                                                 linpred = sum(linpred * weight)
#                                                              ,
#                                                 pred.upper = sum(pred.upper * weight),
#                                                 pred.lower = sum(pred.lower * weight),
                                        #                                                 pred = sum(pred * weight)
                                                 ),
                                             by = .(nearest.threshold, d.nearest.threshold)]
        syndata <- syndata.weighted
    }

    if(transform == FALSE){
        syndata[,":="(linpred = family(model)$linkinv(linpred),
                      linpred.upper = family(model)$linkinv(linpred.upper),
                      linpred.lower = family(model)$linkinv(linpred.lower))
                ]
    }

    return(syndata)
}



    ## for(threshold in unique(syndata$nearest.threshold)){
    ##     p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=prob.damaging, group=post.cutoff.fact,fill=post.cutoff.fact), data = syndata[nearest.threshold == threshold], alpha=0.5)
    ##     p <- p + geom_line(data = syndata[nearest.threshold == threshold],aes(y=linpred, x=prob.damaging,color=post.cutoff.fact,group=post.cutoff.fact))
    ## }

plot.threshold.cutoffs <- function(df, wiki,  partial.plot=NULL){
    if(is.null(partial.plot)){
        p <- ggplot()
    } else {
        p <- partial.plot
    }
    p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=d.nearest.threshold), alpha=0.5, data=df)
    p <- p + geom_line(aes(y=linpred, x=d.nearest.threshold), data=df)
    p <- p + facet_wrap(. ~ nearest.threshold, scales="free_x")
    p <- p + ggtitle(wiki) + xlab("Distance from threshold") + ylab("Prob. reverted")
    return(p)
}


plot.bins <- function(data.plot, partial.plot = NULL){
    if (is.null(partial.plot)){
        partial.plot <- ggplot()
    }
    p <- partial.plot + geom_point(aes(x=bin.mid,y=prob.outcome),data=data.plot) + geom_errorbar(aes(x=bin.mid,ymax=prob.outcome+1.96*se.bern,ymin=prob.outcome-1.96*se.bern),data=data.plot, alpha=0.5, size=0.1)

    return(p)
}

plot.wiki.model <- function(wiki, filename, p=0.05){
    mod1.wiki.file <- paste0("model_1_",wiki,"_bandwidth_stanfit.RDS")
    model <- readRDS(mod1.wiki.file)
    syndata <- gen.syndata.with.predictions(wiki, df.cutoff, model)
    p <- plot.threshold.cutoffs(syndata, wiki, paste0("cutoffs_",wiki,".pdf"))
    return(p)
}

build.regtable <- function(model){
    reg.table.columns <- c("varname","mean","sd","2.5%",'25%',"50%",'75%',"97.5%","Rhat")
    mod.tab  <- as.data.table(summary(model, probs=c(0.025, 0.25, 0.50, 0.75, 0.975)))

    mod.tab$varname <- rownames(data.frame(summary(model)))

    df <- data.table(model$data)
    mod.xtable  <- xtable(mod.tab[,reg.table.columns,with=FALSE], include.rownames=TRUE)
    return (mod.xtable)
}

analyze.model <- function(model.path, name, wiki="all", prob, placebo.shift, transform=TRUE){
    mod <- readRDS(model.path)
    mod.xtable <- build.regtable(mod)
    remember(mod.xtable, paste(name,'xtable',sep='.'))
    df <- as.data.table(mod$data)
    N.by.wiki.cutoff <- df[,.(N=.N,total.weight=sum(weight)), by=.(wiki.db, nearest.threshold, gt.nearest.threshold)]
    remember(N.by.wiki.cutoff, paste(name,'N.by.wiki.cutoff',sep='.'))
    N.by.cutoff <- df[,.(N=.N,total.weight=sum(weight)), by=.(nearest.threshold, gt.nearest.threshold)]

    remember(N.by.cutoff, paste(name,'N.by.cutoff',sep='.'))

    draws  <- as.data.table(mod$stanfit)

    remember(draws,paste(name,'draws',sep='.'))
    remember.plot.data(mod,name,wiki=wiki,prob=prob, placebo.shift=placebo.shift,transform=transform)
}


remember.plot.data <- function(model, name, wiki='all',prob=0.05, n.bins=10, newdata=NULL, placebo.shift=0,transform=TRUE){
    if(is.null(newdata)){
        df <- as.data.table(model$data)
    } else {
        df <- newdata
    }

    me.data.df <- gen.syndata.with.predictions(wiki, df, model, prob=prob, transform=transform, placebo.shift=placebo.shift)
    
    bins.df <- gen.bins.data(exemplar.wiki=wiki, model, n.bins=14, prob=prob, weighted=TRUE)
    remember(me.data.df, paste0(name,".me.data.df"))
    remember(bins.df, paste0(name,".bins.df"))
}

test.plotting <- function(transform=TRUE){
    me.data.df <- gen.syndata.with.predictions(wiki, df, model, prob=prob, transform=transform, placebo.shift=placebo.shift)
    
    bins.df <- gen.bins.data(exemplar.wiki=wiki, model, n.bins=n.bins,example.wiki, prob=prob, weighted=TRUE)
    p <- make.rdd.plot(me.data.df, bins.df, 'test')
    print(p)
    dev.off()
}

make.rdd.plot <- function(me.data.df, bins.df, title){
    p <- plot.bins(bins.df) 

    p <-  plot.threshold.cutoffs(me.data.df, '', partial.plot = p)

    p <- p + ggtitle(title)
    return(p)
}

