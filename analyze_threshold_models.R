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

plot.general.model <- function(exemplar.wiki='enwiki', model=NULL, model.file=NULL, model.name = "", plot.data = TRUE, n.bins=20, prob=0.02){

    if(plot.data == TRUE){
        p <- plot.bins(exemplar.wiki, model, n.bins, prob)
    } else {
        p <- ggplot()
    }

    if(is.null(model)){
        model <- readRDS(model.file)
    }

    syndata <- gen.syndata.with.predictions(exemplar.wiki, df.cutoff, model, prob=prob, transform=TRUE)
    p <- plot.threshold.cutoffs(syndata, paste0(exemplar.wiki,'_',model.name), partial.plot=p)
    filename <-  paste0('overall_model_',exemplar.wiki,'_',model.name,'_','_cutoffs.pdf')
    cairo_pdf(filename)
    print(p)
    dev.off()

    return(p)
}    

plot.bins <- function(exemplar.wiki, model, n.bins, prob, partial.plot = NULL){
        data <- as.data.table(model$data)
#        data <- data[wiki.db == exemplar.wiki]
        threshold.set <- list()

        if (is.null(partial.plot)){
            partial.plot <- ggplot()
        }
            
        for(threshold in unique(data$nearest.threshold)){
            # need to fix the bins so the line up with the threshold
            
            range.pre <- range(data[nearest.threshold == threshold]$d.nearest.threshold)
            
            bins <- c(seq(from=-prob, to=0, by= prob/n.bins*2),seq(from=prob/n.bins*2, to=prob, by= prob/n.bins*2))
            binned <- data.table(bin=bins, threshold=threshold)
            binned <- binned[,next.bin := shift(bin,1,1,type='lead')]
            binned <- binned[,bin.mid := bin + (next.bin-bin)/2]
            binned <- binned[,bin.1 := bin]

            threshold.set[[threshold]]  <-  data[binned, on=.(nearest.threshold=threshold, d.nearest.threshold>bin, d.nearest.threshold<next.bin),nomatch=FALSE]
        }

        data <- rbindlist(threshold.set)

        outcome <- all.vars(model$formula)[1]

        # the weights from the stratified sample don't sum to one since they are between-wiki weights
        data <- data[,":="(N=.N, prob.outcome=weighted.mean(.SD[[outcome]],weight), bin=first(bin.1)) ,by=.(nearest.threshold,bin.mid)]
        
        # rescale the weights so they sum to 1
#        data <- data[,sd.weight := weight/sum(weight)]
        data <- data[, sd.outcome := sum(weight * (.SD[[outcome]] - prob.outcome)^2)/sum(weight),by=.(nearest.threshold, bin.mid)]

        data.plot <- data[,lapply(.SD, first), by=.(nearest.threshold,bin.mid),.SDcols=c('N','prob.outcome','sd.outcome')]
        data.plot <- data.plot[, nearest.threshold := factor(nearest.threshold, levels=c('maybebad','likelybad','verylikelybad'))]
        p <- partial.plot + geom_point(aes(x=-bin.mid,y=prob.outcome),data=data.plot)  + geom_errorbar(aes(x=-bin.mid,ymax=prob.outcome+1.96*sd.outcome,ymin=prob.outcome-1.96*sd.outcome),data=data.plot)
        return(p)

}



gen.syndata.with.predictions <- function(wiki, df.cutoff, model, prob=0.2, transform=TRUE){

    syndata <- gen.synthetic.data(wiki, df.cutoff)

    # drop NA columns that we aren't using right now
    syndata$d.likelygood.threshold = NULL
    syndata$pred.damaging.likelygood = NULL
    syndata$damaging.likelygood.max = NULL

    syndata <- syndata[d.abs.nearest.threshold <= prob]
    syn.ppd <- posterior_predict(model, newdata=syndata)
    syndata$pred <- apply(syn.ppd,2,mean)

    ppd.interval <- predictive_interval(model, newdata=syndata)
                                        # the posterior predictive interval isn't what we want for interpreting statistical significance of differences
    linpred.res <- posterior_linpred(model, newdata=syndata, transform=transform)
    linpred.interval <- t(apply(linpred.res, 2, function(r) quantile(r, c(0.025,0.5, 0.975))))

    syndata$pred.lower <- ppd.interval[,1] 
    syndata$pred.upper <- ppd.interval[,2] 

    syndata$linpred.lower <- linpred.interval[,1] 
    syndata$linpred <- linpred.interval[,2]

    syndata$linpred.upper <- linpred.interval[,3]

    syndata$pred <- apply(syn.ppd,2,mean)

    syndata$nearest.threshold <- factor(syndata$nearest.threshold, levels = c("maybebad","likelybad","verylikelybad"))
    return(syndata)
}



    ## for(threshold in unique(syndata$nearest.threshold)){
    ##     p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=prob.damaging, group=post.cutoff.fact,fill=post.cutoff.fact), data = syndata[nearest.threshold == threshold], alpha=0.5)
    ##     p <- p + geom_line(data = syndata[nearest.threshold == threshold],aes(y=linpred, x=prob.damaging,color=post.cutoff.fact,group=post.cutoff.fact))
    ## }

plot.threshold.cutoffs <- function(syndata, wiki,  partial.plot=NULL){
    if(is.null(partial.plot)){
        p <- ggplot()
    } else {
        p <- partial.plot
    }
    p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=-d.nearest.threshold), alpha=0.5, data=syndata)
    p <- p + geom_line(aes(y=linpred, x=-d.nearest.threshold), data=syndata)
    p <- p + facet_wrap(. ~ nearest.threshold, scales="free_x")
    p <- p + ggtitle(wiki) + xlab("Distance from threshold") + ylab("Prob. reverted")
    return(p)
}

plot.wiki.model <- function(wiki, filename, p=0.05){
    mod1.wiki.file <- paste0("model_1_",wiki,"_bandwidth_stanfit.RDS")
    model <- readRDS(mod1.wiki.file)
    syndata <- gen.syndata.with.predictions(wiki, df.cutoff, model)
    p <- plot.threshold.cutoffs(syndata, wiki, paste0("cutoffs_",wiki,".pdf"))
    return(p)
}

## model.1.enwiki <- readRDS("model_1_enwiki_stanfit.RDS")

## syndata <- gen.synthetic.data("enwiki", df.cutoff)

## syn.ppd <- posterior_predict(model.1.enwiki, newdata=syndata)
## syndata$pred <- apply(syn.ppd,2,mean)
## ppd.interval <- predictive_interval(model.1.enwiki, newdata=syndata)
## # the posterior predictive interval isn't what we want for interpreting statistical significance of differences
## linpred.res <- posterior_linpred(model.1.enwiki,newdata=syndata, transform=TRUE)
## linpred.interval <- t(apply(linpred.res, 2, function(r) quantile(r, c(0.025,0.5, 0.975))))

## syndata$pred.lower <- ppd.interval[,1] 
## syndata$pred.upper <- ppd.interval[,2] 

## syndata$linpred.lower <- linpred.interval[,1] 
## syndata$linpred <- linpred.interval[,2]
## syndata$linpred.upper <- linpred.interval[,3]

## ## we want the 95% credible interval instead

## p <- ggplot(syndata)

## p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=prob.damaging, group=post.cutoff.fact,fill=post.cutoff.fact),alpha=0.5)

## p <- p + geom_line(aes(y=linpred, x=prob.damaging,color=post.cutoff.fact,group=post.cutoff.fact))

## cairo_pdf("cutoffs_enwiki.pdf")
## p
## dev.off()
