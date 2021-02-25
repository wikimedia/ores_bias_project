library(data.table)
setDTthreads(0)
library(ggplot2)
source("helper.R")
source("project_settings.R")
theme_set(theme_bw())

load.cutoffs <- function(){
#    cutoffs  <- fread(file.path(data.dir,"ores_rcfilters_thresholds.csv"))
    cutoffs <-  fread(file.path(data.dir,"ores_rcfilters_thresholds_withdefaults.csv"))
    setnames(cutoffs,old=names(cutoffs), new=gsub('_','\\.',names(cutoffs)))
    cutoffs <- cutoffs[,deploy.dt := parse.date.iso(cutoffs$deploy.dt)]
    cutoffs <- cutoffs[,commit.dt := parse.date.iso(cutoffs$commit.dt)]

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
    ## 1

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
    df <- fread(file.path(data.dir, "wiki_weeks_simplified.csv"),sep=',')
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))
    
    df <- df[,week:=as.POSIXct(week, formay="%Y-%m-%d", tz='UTC')]
#    df <- df[,week:=parse.date.iso(week)]    

    if(is.null(cutoffs)){
        cutoffs <- load.cutoffs()
    }

    # need to do this merge on time as well. 

    df <- df[order(wiki.db,week)]

    df <- df[,og.week := week]

    cols.to.use  <- c("week",
                      "og.week",
                      "deploy.dt",
                      "next.deploy.dt",
                      "has.rcfilters.watchlist",
                      "has.rcfilters",
                      "has.ores",
                      "wiki.db",
                      "anonymous.N.edits",
                      "anonymous.N.reverted",
                      "newcomer.N.edits",
                      "newcomer.N.reverted",
                      "established.N.edits",
                      "established.N.reverted")

    since.first.cutoff <- df[cutoffs,
                             ..cols.to.use,
                             on=.(wiki.db=wiki.db,week>=deploy.dt,week<next.deploy.dt),
                             nomatch=0
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


    ## problem: this should be the /nearest/ deploy dt not the only one.
    df[,weeks.from.deploy.dt := round(difftime(week,deploy.dt,units='weeks'))]
    df[,weeks.from.next.deploy.dt := round(difftime(week,next.deploy.dt,units='weeks'))]
    wikis.to.exclude <- c("mediawikiwiki","test2wiki","testwiki","euwiki")
    df <- df[ !(wiki.db %in% wikis.to.exclude)]
    df <- df[, ":="(month = factor(month(week)), n.week = factor(week(week)))]
    df <- df[, time.days := week - parse.date.iso("2000-01-01 00:00:00")]
    df <- melt(df, id.vars=c("has.ores","has.rcfilters","has.rcfilters.watchlist","wiki.db",'n.week','time.days','week','weeks.from.deploy.dt','deploy.dt'),measure.vars=c("established.N.reverted","established.N.edits","newcomer.N.reverted","newcomer.N.edits","anonymous.N.reverted","anonymous.N.edits"))

    df <- df

    df[,editor.type := sapply(strsplit(as.character(df$variable),'.',fixed=TRUE),function(i) i[1])]
    df[,editor.type := factor(editor.type, levels=c("established","anonymous","newcomer"))]
    df[,edits.or.reverted := sapply(strsplit(as.character(df$variable),'.',fixed=TRUE),function(i) i[length(i)])]


    ## we have duplicate rows, but this will be fixed soon
                                        #df <- unique(df)

    edits  <- df[edits.or.reverted=='edits']
    edits[,edits:=value]
    edits[,value:=NULL]
    edits[,edits.or.reverted:=NULL]
    edits[,variable:=NULL]

    reverts <- df[edits.or.reverted=='reverted']
    reverts[,reverts:=value]
    reverts[,value:=NULL]
    reverts[,edits.or.reverted:=NULL]
    reverts[,variable:=NULL]

    df = edits[reverts,on=.(editor.type,has.ores,has.rcfilters,has.rcfilters.watchlist,wiki.db,n.week,time.days, weeks.from.deploy.dt,deploy.dt)]

                                        # maybe I can do this with arima using the other groups as dummy variables
    df <- df[order(wiki.db,editor.type,week)]
    df <- df[,":="(reverts.l1=shift(reverts,1,type='lag'), edits.l1=shift(edits,1,type='lag')),by=.(wiki.db,editor.type)]
    df <- df[order(wiki.db, editor.type, week)]
    df <- df[,":="(ores.cutoff = (has.ores == FALSE &
                                       shift(has.ores,n=1, type='lead') == TRUE),
                   rcfilters.cutoff = (has.rcfilters == FALSE &
                                             shift(has.rcfilters,n=1, type='lead') == TRUE),
                   rcfilters.watchlist.cutoff = (has.rcfilters.watchlist == FALSE &
                                                       shift(has.rcfilters.watchlist,n=1, type='lead') == TRUE)),
                   by=.(wiki.db,editor.type)
                   ]

    df <- df[,is.cutoff := ((ores.cutoff == TRUE) |
                            (rcfilters.cutoff == TRUE) |
                            (rcfilters.watchlist.cutoff == TRUE))
             ]


    cutoff.rows = df[ is.cutoff==TRUE, .(wiki.db, editor.type, week, nearest.cutoff=week)]
    
    ## let's try out the roll join
    setkey(df, wiki.db, editor.type, week)

    setkey(cutoff.rows, wiki.db, editor.type, week)
    df <- cutoff.rows[df, roll='nearest']
    df <- df[,i.week:=NULL]

    # okay roll joins are sick!

    df <- df[,weeks.from.cutoff := difftime(nearest.cutoff,week,units="weeks")]

    cutoff.types <- df[weeks.from.cutoff==0]

    df <- df[cutoff.types,":="(ores.cutoff = i.ores.cutoff,
                               rcfilters.cutoff = i.rcfilters.cutoff,
                               rcfilters.watchlist.cutoff = i.rcfilters.watchlist.cutoff),
             on=.(wiki.db, editor.type, nearest.cutoff)]
    
   return(df)
 ,}

build.rdd.dbds  <- function(filename="cutoff_revisions_sample.csv", strata_counts="threshold_strata_counts.csv",placebo.shift=0){
    df  <- fread(file.path(data.dir,filename))
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))
    df <- df[,":="(event.timestamp=parse.date.iso(event.timestamp))]
    df <- df[,":="(revert.timestamp=parse.date.iso(revert.timestamp))]
    exclude.wikis <- c("wikidatawiki","eswikibooks", "simplewiki", "eswikiquote")


    #df <- df[,":="(strata=paste(wiki.db, tolower(revision.is.identity.reverted),tolower(has.user.page), tolower(is.controversial), tolower(event.user.is.anonymous), sep='_'))]
#    df <- df[,":="(strata = gsub("_NA","",strata))]
    
## wmhist = wmhist.withColumn('strata',f.concat_ws('_',wmhist.wiki_db,wmhist.revision_is_identity_reverted,wmhist.has_user_page, wmhist.is_controversial,wmhist.event_user_is_anonymous))
    
    date.range <- df[,.(min.date = min(event.timestamp, na.rm=T), max.date = max(event.timestamp, na.rm=T)), by=.(wiki.db)]
    remember(date.range, 'date.range.by.wiki')
    strata.counts  <- fread(file.path(data.dir,strata_counts))

    df <- df[strata.counts,on='strata']
    df <- df[ ! (wiki.db %in% exclude.wikis)]
    df <- add.thresholds(df, filename="historical_thresholds.csv", placebo.shift=placebo.shift)
    df <- df[,':='(over.profiled = (has.user.page == FALSE) | (event.user.is.anonymous == TRUE))]
    return(df)
}


load.rdd.ds <- function(filename="historically_scored_sample.csv", cutoffs=NULL,placebo.shift=0){
    ## if(is.null(cutoffs)){
    ##     cutoffs <- load.cutoffs()
    ## }
    df <- fread(file.path(data.dir, filename),
                select = c(
                    "wiki_db",
                    "commit",
                    "revision_id",
                    "event_timestamp",
                    "anon_new_established",
                    "revision_is_identity_reverted",
                    "time_to_revert",
                    "reverted_in_24h",
                    "prob_damaging",
                    "revscoring_error",
                    "event_user_is_anonymous",
                    "has_user_page",
                    "is_controversial"
                )
                )

# wmhist = wmhist.withColumn('strata',f.concat_ws('_',f.lit('wiki'),wmhist.wiki_db,f.lit('reverted'),wmhist.revision_is_identity_reverted,f.lit('hasup'),wmhist.has_user_page, f.lit('controversial'), wmhist.is_controversial,f.lit('anon'),wmhist.event_user_is_anonymous))

    
    setnames(df, old=names(df), new=gsub('_','\\.',names(df)))

    exclude.wikis <- c("wikidatawiki","eswikibooks", "simplewiki", "eswikiquote")
    df  <- df[ ! (wiki.db %in% exclude.wikis)]

    df <- df[,":="(event.timestamp=parse.date.iso(event.timestamp))]
    df <- df[,":="(revert.timestamp=parse.date.iso(revert.timestamp))]

    df <- df[,":="(strata=paste( "wiki", wiki.db, "reverted", tolower(revision.is.identity.reverted),"hasup",tolower(has.user.page), "controversial", tolower(is.controversial), "anon", tolower(event.user.is.anonymous), sep='_'))]
    df <- df[,":="(strata = gsub("_NA","",strata))]

    df <- df[,":="(event.timestamp=parse.date.iso(event.timestamp),
                   revision.is.identity.reverted = (revision.is.identity.reverted == "True")  | (revision.is.identity.reverted=="true")
                   )]

    strata.counts  <- fread(file.path(data.dir,"strata_counts.csv"))
    

    df <- df[strata.counts,on='strata']
    
    df  <- add.thresholds(df, placebo.shift)
    
    df <- df[,':='(over.profiled = (has.user.page == FALSE) | (event.user.is.anonymous == TRUE))]
     
    ## df <- df[cutoffs,':='(has.ores = i.has.ores,
    ##                       has.rcfilters = i.has.rcfilters,
    ##                       has.rcfilters.watchlist=i.has.rcfilters.watchlist,
    ##                       damaging.likelybad.min = as.numeric(i.damaging.likelybad.min.value),
    ##                       damaging.verylikelybad.min = as.numeric(i.damaging.verylikelybad.min.value),
    ##                       damaging.maybebad.min = as.numeric(i.damaging.maybebad.min.value),
    ##                       damaging.likelygood.max = as.numeric(i.damaging.likelygood.max.value),
    ##                       deploy.dt = i.deploy.dt,
    ##                       pre.cutoff = toupper(pre.cutoff)=='TRUE'
    ##                       ),
    ##          on=.(wiki.db=wiki.db,date=deploy.dt)]

    return(df)
}

add.thresholds  <- function(df, filename="historical_thresholds.csv",placebo.shift){
    thresholds  <- fread(file.path(data.dir, filename),
                         select=c("commit",
                                  "deploy_dt",
                                  "wiki_db",
                                  "damaging_likelybad_max",
                                  "damaging_likelybad_min",
                                  "damaging_likelygood_max",
                                  "damaging_likelygood_min",
                                  "damaging_maybebad_max",
                                  "damaging_maybebad_min",
                                  "damaging_verylikelybad_max",
                                  "damaging_verylikelybad_min",         
                                  "goodfaith_likelybad_max",
                                  "goodfaith_likelybad_min",
                                  "goodfaith_likelygood_max",           
                                  "goodfaith_likelygood_min",
                                  "goodfaith_maybebad_max",             
                                  "goodfaith_maybebad_min",
                                  "goodfaith_verylikelybad_max",
                                  "goodfaith_verylikelybad_min",
                                  "damaging_likelybad_max_value",
                                  "damaging_likelybad_min_value",
                                  "damaging_likelygood_max_value",
                                  "damaging_likelygood_min_value",
                                  "damaging_maybebad_max_value",
                                  "damaging_maybebad_min_value",
                                  "damaging_verylikelybad_max_value",
                                  "damaging_verylikelybad_min_value",         
                                  "goodfaith_likelybad_max_value",
                                  "goodfaith_likelybad_min_value",
                                  "goodfaith_likelygood_max_value",           
                                  "goodfaith_likelygood_min_value",
                                  "goodfaith_maybebad_max_value",             
                                  "goodfaith_maybebad_min_value",
                                  "goodfaith_verylikelybad_max_value",
                                  "goodfaith_verylikelybad_min_value"
                                  )
                         )

    setnames(thresholds, old=names(thresholds), new=gsub('_','\\.',names(thresholds)))
    
    thresholds[,deploy.dt := parse.date.iso(deploy.dt)]

    thresholds <- thresholds[order(deploy.dt)]
    setkey(thresholds, wiki.db, deploy.dt)
    setkey(df, wiki.db, event.timestamp)
    df <- thresholds[df, roll=T]
    df <- transform.threshold.variables(df, placebo.shift)
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

transform.threshold.variables <- function(df, placebo.shift){

    ## sometimes get thresholds in revscoring fails and we got a string back. so we need to call as.numeric
    ## this is okay since the values get set to null and therefore we'll drop the missing values
    df <- df[,":="(damaging.maybebad.min.value = as.numeric(damaging.maybebad.min.value) + placebo.shift,
                   damaging.likelybad.min.value = as.numeric(damaging.likelybad.min.value) + placebo.shift,
                   damaging.verylikelybad.min.value = as.numeric(damaging.verylikelybad.min.value) + placebo.shift,
                   damaging.likelygood.max.value = as.numeric(damaging.likelygood.max.value) + placebo.shift
                   )]

    df <- df[,":="(pred.damaging.maybebad = prob.damaging > damaging.maybebad.min.value,
             d.maybebad.threshold =  prob.damaging - damaging.maybebad.min.value,
             pred.damaging.likelybad = prob.damaging > damaging.likelybad.min.value,
             d.likelybad.threshold = prob.damaging - damaging.likelybad.min.value,
             pred.damaging.verylikelybad = prob.damaging > damaging.verylikelybad.min.value,
             d.verylikelybad.threshold = prob.damaging - damaging.verylikelybad.min.value,
             pred.damaging.likelygood = prob.damaging < damaging.likelygood.max.value,
             d.likelygood.threshold = prob.damaging - damaging.likelygood.max.value
             )
       ]

    df <- df[,":="(d.abs.nearest.threshold = pmin(abs(d.maybebad.threshold),
                                       abs(d.likelybad.threshold),
                                       abs(d.verylikelybad.threshold),na.rm=T))]
             

    df <- df[,":="(nearest.threshold = ifelse(d.abs.nearest.threshold==abs(d.maybebad.threshold),"maybebad",
                                 ifelse(d.abs.nearest.threshold==abs(d.likelybad.threshold),"likelybad",
                                 ifelse(d.abs.nearest.threshold==abs(d.verylikelybad.threshold),"verylikelybad",NA))))]

    df <- df[,":="(d.nearest.threshold = ifelse(nearest.threshold == 'maybebad', d.maybebad.threshold,
                                   ifelse(nearest.threshold == 'likelybad', d.likelybad.threshold,
                                   ifelse(nearest.threshold == 'verylikelybad',d.verylikelybad.threshold,NA))))]

    df <- df[,":="(gt.nearest.threshold = ifelse(nearest.threshold == 'maybebad', pred.damaging.maybebad,
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



to.bool <- function(s){
    return(tolower(s)=='true')
}

parse.strata.name <- function(fields){
    match.list <- list('wiki'='wiki',
                       'reverted'='reverted',
                       'r48'='is.reverted.48h',
                       'r2'='is.reverted.2h',
                       'hasup'='has.up',
                       'controversial'='is.controversial',
                       'anon'='is.anon',
                       'nearest.threshold'='nearest.threshold',
                       'gt.nearerest.threshold'='gt.nearest.threshold')

    bool.vals <- c('reverted','is.reverted.48h','is.reverted.2h','has.up','is.controversial','is.anon','gt.nearest.threshold')

    result <- list()
    for(i in 1:length(fields)){
        if(fields[[i]] %in% names(match.list)){
            varval <- NA
            varname <- match.list[[fields[[i]]]]
            if( (length(fields) >= i+1)){
                if(! (fields[[i+1]] %in% names(match.list)))
                    varval <- fields[[i+1]]
            }
            result[[varname]] <- varval
            if(varname %in% bool.vals)
                result[[varname]] <- to.bool(varval)
        }

    }
    for(key in match.list){
        if(! (key %in% names(result)))
           result[[key]] <- NA
    }
    return(result)
}

