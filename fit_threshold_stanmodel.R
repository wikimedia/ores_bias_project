source("modeling_init.R")
## first lets fit the all pooling model.
## under reasonable assumptions this can identify the LATE of having a revision scored above a threshhold.


formula.1.nowiki.likelyonly  <- revision.is.identity.reverted.bin ~ prob.damaging + post.cutoff.fact + prob.damaging+post.cutoff.fact + pred.damaging.likelybad*post.cutoff.fact

formula.1.nowiki <- update(formula.1.nowiki.likelyonly, ~ . + pred.damaging.verylikelybad*post.cutoff.fact)
formula.1 <- update(formula.1.nowiki, ~ . + wiki.db)
formula.1.slopes  <- update(formula.1, ~ . + wiki.db:prob.damaging)
formula.1.kinks  <- update(formula.1.slopes, ~ . + prob.damaging*pred.damaging.likelybad + prob.damaging*pred.damaging.verylikelybad)

formula.1.more.kinks  <- update(formula.1.kinks, ~ . + prob.damaging*pred.damaging.maybebad:wiki.db + prob.damaging*pred.damaging.likelybad:wiki.db + prob.damaging*pred.damaging.verylikelybad:wiki.db)

## model.1.slopes <- stan_glm(formula=formula.1.slopes, family=binomial(link='logit'), data=as.data.frame(df.cutoff), weights=df.cutoff$weight,iter=2800)
## model.1.kinks <- stan_glm(formula=formula.1.kinks, family=binomial(link='logit'), data=as.data.frame(df.cutoff), weights=df.cutoff$weight,iter=2800)



mod1.enwiki.file <- "model_1_enwiki_stanfit.RDS"
if(!file.exists(mod1.enwiki.file)){
    print(paste0("fitting ", mod1.enwiki.file))
    model.1.enwiki <- stan_glm(formula.1.nowiki.likelyonly, family=binomial(link='logit'), data=df.cutoff[wiki.db=='enwiki'],weights=df.cutoff[wiki.db=='enwiki']$weight,iter=2000,open_progress=TRUE, refresh=10, QR=TRUE)
    saveRDS(model.1.enwiki,mod1.enwiki.file)
}

mod1.all.file <- "model_1_more_kinks_2_stanfit_try2.RDS"
if(!file.exists(mod1.all.file)){
    print(paste0("fitting ",mod1.all.file))
    model.1.more.kinks <- stan_glm(formula.1.more.kinks, family=binomial(link='logit'), data=df.cutoff,weights=df.cutoff$weight,iter=2000,open_progress=TRUE, refresh=10, QR=TRUE, control=list(stepsize=0.1,adapt_delta=0.98))

    saveRDS(model.1.more.kinks,mod1.all.file)
}
