source("modeling_init.R")

model.1.enwiki <- readRDS("model_1_enwiki_stanfit.RDS")

syndata <- gen.synthetic.data("enwiki", df.cutoff)

syn.ppd <- posterior_predict(model.1.enwiki, newdata=syndata)
syndata$pred <- apply(syn.ppd,2,mean)
ppd.interval <- predictive_interval(model.1.enwiki, newdata=syndata)
# the posterior predictive interval isn't what we want for interpreting statistical significance of differences
linpred.res <- posterior_linpred(model.1.enwiki,newdata=syndata, transform=TRUE)
linpred.interval <- t(apply(linpred.res, 2, function(r) quantile(r, c(0.025,0.5, 0.975))))
syndata$pred <- syn.ppd.mean
syndata$pred.lower <- ppd.interval[,1] 
syndata$pred.upper <- ppd.interval[,2] 

syndata$linpred.lower <- linpred.interval[,1] 
syndata$linpred <- linpred.interval[,2]
syndata$linpred.upper <- linpred.interval[,3]

## we want the 95% credible interval instead

p <- ggplot(syndata)

p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,x=prob.damaging, group=post.cutoff.fact,fill=post.cutoff.fact),alpha=0.5)

p <- p + geom_line(aes(y=linpred, x=prob.damaging,color=post.cutoff.fact,group=post.cutoff.fact))

cairo_pdf("cutoffs_enwiki.pdf")
p
dev.off()

