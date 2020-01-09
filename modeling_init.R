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
library(future)
partial <- purrr::partial

## setup.cluster <- function(nodes, jobs.per.node){


mcaffinity(1:detectCores()) ## required and explained below 
options(mc.cores = parallel::detectCores())
theme_set(theme_bw())

start.cluster <- function(nodes, jobs.per.node){
workers <- unlist(lapply(nodes, partial(rep, times=jobs.per.node)))
## # the MPI cluster is better in theory. 
##     cl <- future::makeClusterPSOCK(workers=workers)

##     return(cl)
## }

plan(cluster, workers=workers)
}

library(promises)
overwrite = FALSE

build.rdd.dataset <- function(cutoffs=NULL){
    df <- load.rdd.ds(cutoffs=cutoffs)
    df <- add.rescored.revisions(df)
    df <- transform.threshold.variables(df)
    rcfilters.dates <- df[(has.ores==T) & (has.rcfilters==T) & (has.rcfilters.watchlist==T), .(date=min(date)),by=wiki.db]
    df <- df[,":="(post.cutoff.fact = as.factor(pre.cutoff == FALSE),
               wiki.db = as.factor(wiki.db),
               revision.is.identity.reverted.bin=revision.is.identity.reverted=="TRUE")]
    df.cutoff <- df[rcfilters.dates, on=.(wiki.db,date)]
    return(df.cutoff)
}

if(!(exists("df.cutoff"))){
    df.cutoff <- build.rdd.dataset()
    df.pre <- df.cutoff[pre.cutoff==T]
    df.post <- df.cutoff[pre.cutoff==F]
}

gen.synthetic.data <- function(wiki, df.cutoff){

    syn.cutoffs <- first(df.cutoff[wiki.db==wiki,.(wiki.db,
                                                   damaging.maybebad.min,
                                                   damaging.likelybad.min,
                                                   damaging.verylikelybad.min,
                                                   damaging.likelygood.max,
                                                   revision.is.identity.reverted=NA
                                                       )])

    syndata <- data.table(prob.damaging = 0:10000/10000)

    syndata[, ":="(has.rcfilters.watchlist=FALSE, wiki.db=wiki)]

    syndata2 <- copy(syndata)

    syndata2[,has.rcfilters.watchlist:=TRUE]

    syndata <- rbindlist(list(syndata,syndata2))
    
    syndata <- syndata[syn.cutoffs, on=.(wiki.db)]

    syndata <- transform.threshold.variables(syndata)

    syndata$revision.is.identity.reverted = NULL
    syndata <- syndata[,":="(has.rcfilters.watchlist = as.factor(has.rcfilters.watchlist),
                             post.cutoff = has.rcfilters.watchlist,
                             post.cutoff.fact = as.factor(has.rcfilters.watchlist))]

    return(syndata)
}

do.model <- function(stancall, fit.loo=FALSE, fit.waic=TRUE, output=NULL, overwrite=TRUE, capture.output=FALSE){
    work <- function(QR=TRUE, fit.loo,fit.waic){
        result <- stancall(QR=QR)
        if(fit.loo == TRUE){
            result$loo = loo(result)
        }
        if(fit.waic == TRUE){
            result$waic = waic(result)
        }
        result
    }        

    result <- tryCatch({work(QR=TRUE, fit.loo, fit.waic)},
             error= function(e){
                 print(e)
                 print("trying again with QR=FALSE")
                 tryCatch({
                     work(QR=FALSE, fit.loo, fit.waic)},
                     error = function(e){
                         print(e)
                     }
                     )
             }
             )

    print(output)
    if(!is.null(output) && (overwrite != FALSE)){
        saveRDS(result,output)
    }
    if(capture.output == TRUE){
        return(result)
    } else {
        return(NULL)
    }
}

do.model.async <- function(stancall, output, fit.loo=FALSE, fit.waic=TRUE, overwrite=overwrite, globals=c()){
    # to make things simple, we put everything we need into the function :)
    f <- future({
        library('parallel')
        mcaffinity(1:detectCores()) ## required and explained below 
        options(mc.cores = parallel::detectCores())
        do.model(stancall, fit.loo=fit.loo, fit.waic=fit.waic, output=output)
    },
    globals=c('filename', "f.established","f.newcomer",'f.anonymous','loo','waic',"df","chains","overwrite", "warmup","sampling", "stan_glm", "do.model","stancall", "iter", "chains","model.path", "ns", "output", "fit.loo","fit.waic","warmup", globals),
    label=output
    )
    return(f)
}


## someday scale out jobqueue into a cran package.
## would be great for jobs to have names

setRefClass("JobQueue",
            fields=list(waiting='list',running='list', done='list'))

jobqueue <- function(){
    jobs <- new("JobQueue")
    return(jobs)
}

enqueue.job <- function(object, job, callback){
    object$waiting <- c(object$waiting, list(list(job=job, callback=callback)))
    return(object)
}

start.next.job <- function(object) {
        

    if(length(object$waiting) == 0){
        return(object)
    }
        
    job <- object$waiting[[1]]
    object$waiting <- object$waiting[-1]
    promise <- job$job()
    object$running <-  c(object$running, promise)

    callback.base  <- function(){
    if(length(object$waiting) > 0){
        object <- start.next.job(object)
    }
    object$done <- c(object$done, promise)

    # labels must be unique
    done.labels <- unlist(lapply(object$done, function(j) j$label))
    running.labels <- unlist(lapply(object$running, function(j) j$label))
    running.idx.remove <- which(running.labels %in% done.labels, useNames=FALSE)
    object$running <- object$running[-1*running.idx.remove]
    
    print(paste0("jobs waiting:",length(object$waiting)))
    print(paste0("jobs running:",length(object$running)))
    print(paste0("jobs done:",length(object$done)))

    return(object)
    }
    
    callback.success <- function(value) {
        object <- callback.base()
        job$callback(value)
        return(object)
    }

    callback.failure <- function(error) {
        object <- callback.base()
        print(error)
        return(object)
    }
    
    then(promise,
         onFulfilled = callback.success,
         onRejected = callback.failure
         )


    return(object)
}

run.jobs <- function(object){
    for(i in 1:n){
        object <- start.next.job(object)
    }
    return(object)
}

models.path <- "/gscratch/comdata/users/nathante/ores_bias_project/models"
