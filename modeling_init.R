library(rstanarm)
#xlibrary(lme4)
library(parallel)
library(future)
library(ggplot2)
library(gridExtra)
library(grid)
library(data.table)
source("helper.R")
source("ob_util.R")
source("RemembR/R/RemembeR.R")
print("loaded libraries")

partial <- purrr::partial

## setup.cluster <- function(nodes, jobs.per.node){

parallel::mcaffinity(1:detectCores()) ## required and explained below 
options(mc.cores = parallel::detectCores())
theme_set(theme_bw())

start.cluster <- function(jobs.per.node, nodes=NULL){

if(is.null(nodes)){
    nodes <- system2("scontrol", "show hostnames", stdout=TRUE)
} 

workers <- unlist(lapply(nodes, partial(rep, times=jobs.per.node)))
## # the MPI cluster is better in theory. 
##     cl <- future::makeClusterPSOCK(workers=workers)

##     return(cl)
## }
if(length(unique(workers)) > 1)
    plan(cluster, workers=workers)
else
    plan(multiprocess, workers=length(workers))
}

library(promises)
overwrite = FALSE

build.rdd.dataset <- function(cutoffs=NULL){
    df <- load.rdd.ds()
    df <- transform.threshold.variables(df)

    remember(df[!(is.na(prob.damaging)) & !(is.na(d.nearest.threshold)), .N, by=.(wiki.db, anon.new.established)], 'missing.thresholds.scores.table')

    df <- df[!(is.na(prob.damaging)) & !(is.na(d.nearest.threshold))]
    return(df)
}

gen.synthetic.data <- function(wiki, df.cutoff, dv, n=100, prob = 0.03, placebo.shift=0){

    syn.cutoffs <- first(df.cutoff[wiki.db==wiki,.(wiki.db,
                                                   damaging.maybebad.min.value,
                                                   damaging.likelybad.min.value,
                                                   damaging.verylikelybad.min.value
                                                       )])

    

    syn.cutoffs[[dv]] <- NA

    thresholds = unlist(lapply(unique(df.cutoff$nearest.threshold), function(t) rep(t, n)))

    syndata <- data.table(d.nearest.threshold = rep(seq(-prob,prob,length.out=n),),
                          nearest.threshold=thresholds,
                          damaging.maybebad.min.value=rep(syn.cutoffs$damaging.maybebad.min.value,n),
                          damaging.likelybad.min.value=rep(syn.cutoffs$damaging.likelybad.min.value,n),
                          damaging.verylikelybad.min.value=rep(syn.cutoffs$damaging.verylikelybad.min.value,n),
                          wiki.db = rep(syn.cutoffs$wiki.db,n))

    syndata[nearest.threshold=='maybebad',threshold:=damaging.maybebad.min.value]
    syndata[nearest.threshold=='likelybad',threshold:=damaging.likelybad.min.value]
    syndata[nearest.threshold=='verylikelybad',threshold:=damaging.verylikelybad.min.value]
    syndata[,d.abs.nearest.threshold := abs(d.nearest.threshold)]
    syndata[,prob.damaging:=threshold+d.nearest.threshold]
    syndata[,gt.nearest.threshold:=d.nearest.threshold>0]

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
