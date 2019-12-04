library(sandwich)
library(sjPlot)
library(lmtest)
library(rstanarm)
options(mc.cores = parallel::detectCores())
source("ob_util.R")
source("formulas.r")

df <- load_ts_ds()

set.seed(123)

df <- prepare.df(df)
wiki.stats <- prepare.wikistats(df)

## drop 3 wikis that don't have any ns4 edits
wiki.stats <- wiki.stats[ns4.edits != 0]
df <- df[wiki.db %in% wiki.stats$wiki.db]

# the dv for model1 is ttr
## scale everything
## view count and active editors are super correlated with reverts

# 48 hours for non-reverted as a hueristic for "reverted_for_damage"

# add edits by bots to the treatment model

## scale the numeric variables
to.scale  <- which(sapply(wiki.stats,function(s) is.numeric(s)))
wiki.stats <- wiki.stats[,(to.scale) := lapply(.SD, scale), .SDcols=names(to.scale)]

treated.model.1 <- stan_glm(treatment.formula, family=binomial(link='logit'), data=as.data.frame(wiki.stats), na.action = na.fail)

wiki.stats[,treated.odds:=exp(predict(treated.model.1,wiki.stats))]
wiki.stats[,treated.probs:=treated.odds / (1 + treated.odds)]
wiki.stats[treated==T,ip.weight:= 1/ treated.probs]
wiki.stats[treated==F,ip.weight:= 1/ (1-treated.probs)]

## huston we have a problem with perfect seperation so we want to use the firth-penalized liklihood instead

# add the IP-weights to the df
df <- merge(df,wiki.stats,by=c("wiki.db"),how='left outer',suffixes=c('','.y'))

# let's check if excluding the wikis with the fewest reverts changes things
# df = df[N_reverts >= median(N_reverts)]

#for this test we'll use random cutoffs for each wiki 

# we have clustered errors
# H1 geom_mean_ttr
# DID only 

mod1_file <- "mod1_did_stanmod.RDS"
if(! file.exists(mod1_file)){
    mod1.did <- stan_glmer(m1.formula.stan,as.data.frame(df),family=gaussian(),QR=T,refresh=50)
    saveRDS(mod1.did, mod1_file)
}

# with IP weights
mod1.ip <- stan_glmer(m1.formula.stan, as.data.frame(df), family=gaussian(), QR=T, weights = df$ip.weight, refresh=50)
saveRDS(mod1.ip,"mod1_ip_stanmod.RDS")

coeftest(mod1.ip,vcov.ip)

# doubly robust 
# we don't need sandwhich estimators when we have random effects
mod1.dr <- stan_glmer(m1.formula.dr.stan, as.data.frame(df), family=gaussian(), QR=T, weights=df$ip.weight, refresh=50)
saveRDS(mod1.dr,"mod1_dr_stanmod.RDS")
coeftest(mod1.dr,vcov.dr)

summary(mod1_ip)

summary(mod1_dr)

# H1a number of reverts
# DID only

mod2.did <- stan_glmer.nb(m2.formula.stan, as.data.frame(df), QR=T, refresh.50)

saveRDS(object=mod2.did,file = "ores_bias_data/mod2_did.RDS")

coeftest(mod2.did,vcov.dr)

mod2.ip <- stan_glmer.nb(m2.formula.stan, as.data.fram(df), QR=T, weights=df$ip.weight, refresh=50)
saveRDS(object=mod2.ip,file = "ores_bias_data/mod2_ip.RDS")

coeftest(mod2.ip,vcov.ip)

# doubly robust
mod2.dr <- stan_glmer.nb(m2.formula.dr.stan, as.data.frame(df), weights = df$ip.weight, QR=T, refresh=50)
saveRDS(object=mod2.dr,file = "ores.bias.data/mod2.dr.RDS")

# DID only
# df2 <- df[user.week.revert.cv != 0]

mod3.did <- stan_glm(m3.formula.stan,as.data.frame(df), family=gaussian(link='log'), QR=T, refresh=50)

saveRDS(object=mod3.did,file = "ores.bias.data/mod2.did.RDS")

coeftest(mod3.did,vcov.did)

# IP weights
mod3.ip <- stan_glm(m3.formula.stan, as.data.frame(df), weights = df2$ip.weight, family=gaussian(link='log'), QR=T,refresh=50)

saveRDS(object=mod3.ip,file = "ores.bias.data/mod2.ip.RDS")
coeftest(mod3.ip,vcov.did)

# doubly robust
mod3.dr <- stan_glm(formula=m3.formula.dr.stan, as.data.frame(df), weights = df2$ip.weight, family=gaussian(link='log'), QR=T, refresh=50)

saveRDS(object=mod3.dr,file = "ores.bias.data/mod2.dr.RDS")
