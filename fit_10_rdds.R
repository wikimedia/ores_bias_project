p <- 0.03
iter <- 2000
chains <- 4
source('fit_rdds_base.R')
options(future.globals.maxSize=1000*1000*1000*10)
start.cluster(jobs.per.node=3,nodes=c("n2347"))

#adopted.wikis <- sapply(unique(df$wiki.db), check.adoption)

#df <- df[wiki.db %in% names(adopted.wikis)[adopted.wikis]]

#remember(adopted.wikis, 'adopted.wikis')

# fit
f1 %<-% fit.model(dta=df, name="adoption.check", form = H1.formula)

# fit
f2 %<-% fit.model(df[event.user.is.anonymous==TRUE], name="anon.is.reverted", form = H1.formula, drop.verylikelybad=TRUE)

# fit
f3 %<-% fit.model(df[event.user.is.anonymous==FALSE], name="non.anon.is.reverted", form = H1.formula, drop.verylikelybad=TRUE)

#fit
f4 %<-% fit.model(df[(has.user.page==FALSE) & (event.user.is.anonymous == FALSE)], name="no.user.page.is.reverted", form = H1.formula, drop.verylikelybad=TRUE)

#fit 
f5 %<-% fit.model(df[(has.user.page==TRUE) & (event.user.is.anonymous == FALSE)], name="user.page.is.reverted", form = H1.formula, drop.verylikelybad = TRUE)

f6 %<-% fit.model(revert.df, name="all.is.controversial", form = H2.formula, drop.verylikelybad=TRUE)

f7 %<-% fit.model(revert.df[event.user.is.anonymous==TRUE], name = 'anon.is.controversial', form=H2.formula, drop.verylikelybad=TRUE)

f8 %<-% fit.model(revert.df[event.user.is.anonymous==FALSE], name = 'non.anon.is.controversial', form=H2.formula, drop.verylikelybad=TRUE)

f9 %<-% fit.model(revert.df[(has.user.page==FALSE) & (event.user.is.anonymous == FALSE)], name = 'no.user.page.is.controversial', form=H2.formula, drop.verylikelybad=TRUE)

f10 %<-% fit.model(revert.df[(has.user.page==TRUE) & (event.user.is.anonymous == FALSE)], name = 'user.page.is.controversial', form=H2.formula, drop.verylikelybad=TRUE) #

resolve(globalenv(),result=F)

