if(! exists("QR"))
    QR <- TRUE
if(! exists("chains"))
    chains <- 12
if(! exists("iter"))
    iter <- 2000
if(! exists("warmup"))
    warmup <- 1000
if(! exists("refresh"))
    refresh <- 100
if(! exists("p"))
    p  <- 0.03

formula.rhs.base <- . ~ nearest.threshold:d.nearest.threshold + nearest.threshold:gt.nearest.threshold + nearest.threshold:d.nearest.threshold:gt.nearest.threshold + nearest.threshold 
formula.rhs.kinkless <- . ~ nearest.threshold:d.nearest.threshold:wiki.db + nearest.threshold:gt.nearest.threshold:wiki.db +  nearest.threshold + wiki.db + nearest.threshold:wiki.db

formula.rhs <- update(formula.rhs.base,  ~ . + wiki.db + nearest.threshold:wiki.db)

H1.formula <- update( reverted.in.48h~ ., formula.rhs)
H1.formula.kinkless <- update( reverted.in.48h~ ., formula.rhs.kinkless)
H2.formula <- update(is.controversial ~ ., formula.rhs)
H2.formula.kinkless <- update(is.controversial ~ ., formula.rhs.kinkless)
wiki.adoption.formula <- update(revision.is.identity.reverted ~ ., formula.rhs.base)
H2.onewiki.formula <- update(is.controversial ~ ., formula.rhs.base)

prepare.model  <- function(dta, name, form, do.remember=TRUE){
    obs.per.wiki.threshold <- dta[,.(.N),by=.(wiki.db, nearest.threshold)]
    obs.per.wiki.threshold <- obs.per.wiki.threshold[N >= min.obs.per.wiki.threshold]
    thresholds.per.wiki <- obs.per.wiki.threshold[,.(.N), by=.(wiki.db)]
    included.wikis <- thresholds.per.wiki[N==3]$wiki.db
    excluded.wikis <- thresholds.per.wiki[N!=3]$wiki.db
## excluded.wikis <- c()
##     ## drop wikis with less than 100 observations
##     for(wiki in unique(dta$wiki.db)){
##         for(threshold in unique(dta$nearest.threshold)){
##             n.obs.below <- nrow(dta[ (wiki.db == wiki) &
##                                      (nearest.threshold == threshold) &


##             n.obs.above  <- nrow(dta[ (wiki.db == wiki) &
##                                      (nearest.threshold == threshold) &
##                                      (gt.nearest.threshold == TRUE)])
##             if( (n.obs.below < min.obs.per.wiki.threshold) &
##                 (n.obs.above < min.obs.per.wiki.threshold)){
##                 excluded.wikis <- c(excluded.wikis, wiki)
##             }
##         }
##     }

    if(do.remember == TRUE){
        remember(excluded.wikis,
                 paste(name,'excluded.wikis',sep='.'))

        remember(included.wikis,
                 paste(name,'included.wikis',sep='.'))

    }

    dta  <- dta[wiki.db %in% included.wikis]
    return(dta)
} 

fit.model  <- function(dta, name, form, do.remember=TRUE){
#    mcaffinity(1:detectCores()) ## required and explained below 
    options(mc.cores = parallel::detectCores())

    dta  <- prepare.model(dta,name,form, do.remember)

    assign("dta",dta,envir=globalenv())

    #rescale weight so it sums to N
    dta <- dta[,weight := weight/sum(weight)*.N]
    dta  <- data.frame(dta)
    mod <- stan_glm(formula=form,
                    family=binomial(link='logit'),
                    chains=chains,
                    data=dta,
                    weights=dta[['weight']],
                    iter=iter,
                    warmup=warmup,
                    refresh=refresh,
                    QR=QR
                    )

    saveRDS(mod, file.path("/gscratch/comdata/users/nathante/ores_bias_project/models", paste(name,"stanmod","RDS", sep='.')))
    return(mod)
} 


