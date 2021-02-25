placebo.shift <- 0
source('analyze_threshold_models.R')
example.wiki = 'enwiki'
models.dir <- 'models'
# 1 
print("mod.adoption")
analyze.model(file.path(models.dir,"adoption.check.stanmod.RDS"),"mod.adoption",example.wiki,0.03,0)

#2 
print("mon.anon.reverted")
analyze.model(file.path(models.dir,"anon.is.reverted.stanmod.RDS"),"mod.anon.reverted",example.wiki,0.03,0)

#3
print("mod.non.anon.reverted")
analyze.model(file.path(models.dir,"non.anon.is.reverted.stanmod.RDS"),'mod.non.anon.reverted',example.wiki,0.03,0)

#4 
print("mod.no.user.page.reverted")
analyze.model(file.path(models.dir,"no.user.page.is.reverted.stanmod.RDS"),'mod.no.user.page.reverted',example.wiki,0.03,0)

#5
print('mod.user.page.reverted')
analyze.model(file.path(models.dir,"user.page.is.reverted.stanmod.RDS"),'mod.user.page.reverted',example.wiki,0.03,0)

#6
print('mod.all.controversial')
analyze.model(file.path(models.dir,"all.is.controversial.stanmod.RDS"),'mod.all.controversial',example.wiki,0.03,0)

#7
print('mod.non.anon.controversial')
analyze.model(file.path(models.dir,"non.anon.is.controversial.stanmod.RDS"),'mod.non.anon.controversial',"all",0.03,0)

#8
print('mod.anon.controversial')
analyze.model(file.path(models.dir,"anon.is.controversial.stanmod.RDS"),'mod.anon.controversial',example.wiki,0.03,0)

#9
print('mod.user.page.controversial')
analyze.model(file.path(models.dir,"user.page.is.controversial.stanmod.RDS"),'mod.user.page.controversial',example.wiki,0.03,0)

#10
print('mod.no.user.page.controversial')
analyze.model(file.path(models.dir,"no.user.page.is.controversial.stanmod.RDS"),'mod.no.user.page.controversial',example.wiki,0.03,0)

                                        #value(f)
## mod.non.anon.controversial <- readRDS("models/non.anon.is.controversial.stanmod.RDS")

## plot.general.model(exemplar.wiki='enwiki',model=mod.non.anon.controversial, model.name='Model H2.0. Predicting if revert is controversial, non-anonymous users', plot.data=TRUE, n.bins=20, prob=0.03, placebo.shift=placebo.shift)

## mod.anon <- readRDS("models/stan_rdd_anonymous.RDS")
## mod.established <- readRDS("models/stan_rdd_established.RDS")
## mod.newcomer <- readRDS("models/stan_rdd_newcomer.RDS")


## plot.general.model(exemplar.wiki='enwiki',model=mod.anon, model.name='anonymous', plot.data=TRUE, n.bins=20, prob=0.03, placebo.shift=placebo.shift)

## plot.general.model(exemplar.wiki='enwiki',model=mod.newcomer, model.name='newcomer', plot.data=TRUE, n.bins=20, prob=0.03, placebo.shift=placebo.shift)
## plot.general.model(exemplar.wiki='enwiki',model=mod.established, model.name='established', plot.data=TRUE, n.bins=20, prob=0.03, placebo.shift=placebo.shift)

## plot.bins(exemplar.wiki='enwiki',model=mod.anon, n.bins=10, prob=0.03, placebo.shift=placebo.shift) + facet_wrap(~nearest.threshold)
## plot.bins(exemplar.wiki='all',model=mod.newcomer, n.bins=10, prob=0.03, placebo.shift=placebo.shift) + facet_wrap(~nearest.threshold)
## plot.bins(exemplar.wiki='all',model=mod.established, n.bins=10, prob=0.03, placebo.shift=placebo.shift) + facet_wrap(~nearest.threshold)

## p <- plot.smoother(exemplar.wiki='all',model=mod., n.bins=10, prob=0.03, partial.plot=p)

## p + facet_wrap(~nearest.threshold)



## mod.established <- readRDS("models/stan_rdd_established.RDS")
## mod.newcomer <- readRDS("models/stan_rdd_newcomer.RDS")

## plot.general.model(exemplar.wiki='enwiki',model=mod.established, prob=p, n.bins=20, plot.data=T)

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
