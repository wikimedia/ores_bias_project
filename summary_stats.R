#source('fit_rdds_base.R')
library(data.table)
source("RemembR/R/RemembeR.R")
source("ob_util.R")

change.remember.file("rnr_remember.RDS")


## apparantly there was a bug in thresholding by whether they are flagged and we didn't do this in the end.
## it's okay though we don't need to indicate whether it was over the threshold or not. 

sc <- fread("data/threshold_strata_counts.csv") #db_scores_

fields <- strsplit(sc$strata,'_')

df <- rbindlist(lapply(fields, parse.strata.name))

df <- cbind(sc,df)

df <- df[!(wiki %in% c('eswikiquote','simplewiki','eswikibooks','wikidatawiki'))]

# we also removed these nas when we fit models
sc2 <- fread("data/threshold_strata_counts_vlbfix.csv") #db_scores_

fields2 <- strsplit(sc2$strata,'_')

df2 <- rbindlist(lapply(fields2, parse.strata.name))

df2 <- cbind(sc2,df2)

df <- df[nearest.threshold != 'verylikelybad']
df2 <- df2[nearest.threshold == 'verylikelybad']
df <- rbind(df,df2)
df <- df[!is.na(is.anon) & !is.na(is.reverted.48h)]

by.editor.type <- df[,.(N=sum(count)),by=.(has.up,is.anon,nearest.threshold)]
by.editor.type <- by.editor.type[!(has.up & is.anon)]
by.editor.type[(has.up==TRUE),editor.type:='Reg. User Page']
by.editor.type[(has.up==FALSE) & (is.anon==FALSE),editor.type:='Reg. No User Page']
by.editor.type[(has.up==FALSE) & (is.anon==TRUE),editor.type:='Unregistered']
by.editor.type[,prop := N / sum(N),by=.(nearest.threshold)]

#by.editor.type <- by.editor.type[nearest.threshold != 'verylikelybad']
by.editor.type <- by.editor.type[,nearest.threshold := factor(nearest.threshold,levels=c("maybebad","likelybad","verylikelybad"))]
by.editor.type <- by.editor.type[order(nearest.threshold,editor.type)]

remember(by.editor.type,'summary.editors')

by.edit.type <- df[,.(N=sum(count)),by=.(is.reverted.48h,is.controversial,nearest.threshold)]
by.edit.type <- by.edit.type[is.reverted.48h==FALSE,edit.type:="not reverted"]
by.edit.type <- by.edit.type[(is.reverted.48h==TRUE) & (is.controversial==FALSE),edit.type:="rev. not controversial"]
by.edit.type <- by.edit.type[(is.reverted.48h==TRUE) & (is.controversial==TRUE),edit.type:="rev. controversial"]
by.edit.type <- by.edit.type[,nearest.threshold := factor(nearest.threshold,levels=c("maybebad","likelybad","verylikelybad"))]
by.edit.type <- by.edit.type[,prop := N / sum(N), by=.(nearest.threshold)]
by.edit.type <- by.edit.type[order(nearest.threshold,edit.type)]
#by.edit.type <- by.edit.type[nearest.threshold != 'verylikelybad']

by.edit.type <- by.edit.type[order(nearest.threshold,edit.type)]
remember(by.edit.type,'summary.edits')

by.edit.and.editor.type <- df[,.(N=sum(count)),by=.(is.reverted.48h,is.controversial,has.up,is.anon,nearest.threshold)]

by.edit.and.editor.type <- by.edit.and.editor.type[!(has.up & is.anon)]
by.edit.and.editor.type[(has.up==TRUE),editor.type:='Reg. User Page']
by.edit.and.editor.type[(has.up==FALSE) & (is.anon==FALSE),editor.type:='Reg. No User Page']
by.edit.and.editor.type[(has.up==FALSE) & (is.anon==TRUE),editor.type:='Unregistered']
#by.edit.and.editor.type <- by.edit.and.editor.type[nearest.threshold != 'verylikelybad']
by.edit.and.editor.type <- by.edit.and.editor.type[,nearest.threshold := factor(nearest.threshold,levels=c("maybebad","likelybad","verylikelybad"))]
by.edit.and.editor.type <- by.edit.and.editor.type[is.reverted.48h==FALSE,edit.type:="not reverted"]
by.edit.and.editor.type <- by.edit.and.editor.type[(is.reverted.48h==TRUE) & (is.controversial==FALSE),edit.type:="rev. not controversial"]
by.edit.and.editor.type <- by.edit.and.editor.type[(is.reverted.48h==TRUE) & (is.controversial==TRUE),edit.type:="rev. controversial"]
by.edit.and.editor.type[,prop.threshold := N / sum(N),by=.(nearest.threshold)]
by.edit.and.editor.type[,prop.edit.type := N / sum(N),by=.(nearest.threshold,edit.type)]
by.edit.and.editor.type[,prop.editor.type := N / sum(N),by=.(nearest.threshold,editor.type)]

by.edit.and.editor.type <- by.edit.and.editor.type[order(nearest.threshold,edit.type,editor.type)]
remember(by.edit.and.editor.type,'summary.edits.and.editors')
