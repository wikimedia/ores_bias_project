#! /usr/bin/Rscript
p <- 0.03
iter <- 2000
chains <- 4

sample.filename <- "cutoff_revisions_sample_vlbfix.csv"
strata.counts.filename <- "threshold_strata_counts_vlbfix.csv"
source("RemembR/R/RemembeR.R")
change.remember.file("refit_vlb.RDS")

source("fit_rdds_base.R")

df <- df[nearest.threshold=='verylikelybad']
revert.df <- revert.df[nearest.threshold=='verylikelybad']
#df[(nearest.threshold=='verylikelybad')&(wiki.db=='enwiki'),.(.N),by=.(gt.nearest.threshold,count,strata)]

options(future.globals.maxSize=1000*1000*1000*10)
start.cluster(jobs.per.node=7)

rhs.formula.one.threshold <- . ~ d.nearest.threshold + gt.nearest.threshold + d.nearest.threshold:gt.nearest.threshold + wiki.db
H1.formula.one.threshold <- update(reverted.in.48h ~ ., rhs.formula.one.threshold)
H2.formula.one.threshold <- update(is.controversial ~ ., rhs.formula.one.threshold)

f1 %<-% fit.model(dta=df, name="adoption.check.vlb", form = H1.formula.one.threshold)

f2 %<-% fit.model(df[event.user.is.anonymous==TRUE], name="anon.is.reverted.vlb", form = H1.formula.one.threshold)

f3 %<-% fit.model(df[event.user.is.anonymous==FALSE], name="non.anon.is.reverted.vlb", form = H1.formula.one.threshold)

f4 %<-% fit.model(df[(has.user.page==FALSE) & (event.user.is.anonymous == FALSE)], name="no.user.page.is.reverted.vlb", form = H1.formula.one.threshold)

f5 %<-% fit.model(df[(has.user.page==TRUE) & (event.user.is.anonymous == FALSE)], name="user.page.is.reverted.vlb", form = H1.formula.one.threshold)

#f6 %<-% fit.model(revert.df, name="all.is.controversial", form = H2.formula.one.threshold)

f7 %<-% fit.model(revert.df[event.user.is.anonymous==TRUE], name = 'anon.is.controversial.vlb', form=H2.formula.one.threshold)

f8 %<-% fit.model(revert.df[event.user.is.anonymous==FALSE], name = 'non.anon.is.controversial.vlb', form=H2.formula.one.threshold)

f9 %<-% fit.model(revert.df[(has.user.page==FALSE) & (event.user.is.anonymous == FALSE)], name = 'no.user.page.is.controversial.vlb', form=H2.formula.one.threshold)

f10 %<-% fit.model(revert.df[(has.user.page==TRUE) & (event.user.is.anonymous == FALSE)], name = 'user.page.is.controversial.vlb', form=H2.formula.one.threshold) #

resolve(globalenv(),result=F)

