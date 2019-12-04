## this file has the frequentist analysis
library(sandwich)
library(sjPlot)
library(lmtest)
library(MASS)
library(aod)
options(mc.cores = parallel::detectCores())
set.seed(1337)
source("ob_util.R")
source("formulas.r")

df <- load_ts_ds()

df <- prepare.df(df)
wiki.stats <- prepare.wikistats(df)

## drop 3 wikis with no ns4 edits
wiki.stats <- wiki.stats[ns4.edits != 0]

# drop 1 wiki with no reverts
wiki.stats <- wiki.stats[n.reverts != 0]

# drop several wikis with no anon reverts
wiki.stats <- wiki.stats[n.reverteds.anonymous != 0]
wiki.stats <- wiki.stats[n.reverteds.newcomer != 0]

# drop several wikis with no anon edits to ns4

df <- df[wiki.db %in% wiki.stats$wiki.db]
# the dv for model1 is ttr
## scale everything
## view count and active editors are super correlated with reverts

# 48 hours for non-reverted as a hueristic for "reverted_for_damage"

# add edits by bots to the treatment model
# we don't include ns4.reverts be

treated.model.1 = glm(data =wiki.stats, treatment.formula , family=binomial(link='logit'),na.action = na.fail)

## huston we have a problem with perfect seperation so we want to use the firth-penalized liklihood instead

# turns out that logistf leads to smaller weights than the bayesian approach
res <- add_ip_weights(df,wiki.stats)
df  <- res$df
wiki.stats  <- res$wiki.stats

# let's check if excluding the wikis with the fewest reverts changes things
# df = df[N_reverts >= median(N_reverts)]

#for this test we'll use random cutoffs for each wiki 

# we have clustered errors
# H1 geom_mean_ttr

##DID only 

mod1.did <- lm(m1.formula,df)

vcov.did <- sandwich::vcovCL(mod1.did,df$wiki.db)

coeftest(mod1.did,vcov.did)

##with IP weights
mod1.ip <- lm(df,formula= m1.formula, weights = df$ip.weight)

vcov.ip <- sandwich::vcovCL(mod1.ip,df$wiki.db)

coeftest(mod1.ip,vcov.ip)

#doubly robust 
mod1.dr <- lm(df, formula= m1.formula.dr, weights=df$ip.weight)

vcov.dr <- sandwich::vcovCL(mod1.dr,df$wiki.db)

coeftest(mod1.dr,vcov.dr)

mod2_did <- glm.nb(data=df, formula=m2.formula)
 
saveRDS(object=mod2_did,file = "ores_bias_data/mod2_did.RDS")

vcov.did <- sandwich::vcovCL(mod2_did,df$wiki.db)

coeftest(mod2_did,vcov.did)

mod2_ip <- glm.nb(data=df, formula=m2.formula, weights=df$ip_weight)
saveRDS(object=mod2_ip,file = "ores_bias_data/mod2_ip.RDS")

vcov_ip <- sandwich::vcovCL(mod2_ip,df$wiki.db)

coeftest(mod2_ip,vcov.did)

# doubly robust
mod2_dr_pois <- glm(data=df,formula=m2.formula.dr, weights = df$ip_weight, family=poisson())

mod2_dr <- glm.nb(data=df, formula=m2.formula.dr, weights = df$ip_weight, start=coef(mod2_dr_pois))
saveRDS(object=mod2_dr,file = "ores_bias_data/mod2_dr.RDS")


summary(mod2_did)

summary(mod2_ip)

summary(mod2_dr)

summary(mod2a)

mod2 <- lm(df, formula="N_reverts_demeaned ~ 1 + treated + week_factor + treated_with_ores")

summary(mod2)

# H2 inequality

names(df)

# there's still a problem with the inequality measures

qplot(log(df$revert_hhi))

df[user_week_revert_cv == 0,.(wiki_db,any(treated), mean(N_reverts)),by='wiki_db']

# DID onlyh
# we should be using a zero-inflated model here
library(crch)
df2 <- df[user.week.revert.cv != 0]
mod3.did <- glm(df2, formula=m3.formula,family=gaussian(link='log'))

vcov.did <- sandwich::vcovCL(mod3.did,df[user.week.revert.cv!=0]$wiki.db)

coeftest(mod3.did,vcov.did)

# IP weights
mod3.ip <- glm(df2, formula=m3.formula, weights = df2$ip.weight, family=gaussian(link='log'))

vcov.did <- sandwich::vcovCL(mod3.ip,df[user.week.revert.cv!=0]$wiki.db)

coeftest(mod3.ip,vcov.did)

# doubly robust
mod3.dr <- glm(df2, formula=m3.formula.dr, weights = df2$ip.weight, family=gaussian(link='log'))

vcov.dr <- sandwich::vcovCL(mod3.dr,df$wiki.db)

coeftest(mod3.dr,vcov.dr)

mod4.did <- glm.nb(df,formula=m4.formula)

vcov.did <- sandwich::vcovCL(mod4.did,df$wiki.db)

coeftest(mod4.did,vcov.did)

mod4.ip <- glm.nb(df,formula=m4.formula, weights=df$ip.weight)

vcov.ip <- sandwich::vcovCL(mod4.ip,df$wiki.db)

coeftest(mod4.ip,vcov.did)


mod4.dr <- glm.nb(df, formula=m4.formula.dr, weights=df$ip.weight)

mod5.did <- glm.nb(df,formula=m5.formula)

vcov.ip <- sandwich::vcovCL(mod5.ip,df$wiki.db)

coeftest(mod5.ip,vcov.ip)

mod5.did <- glm.nb(df,formula=m4.formula)

vcov.did <- sandwich::vcovCL(mod5.did,df$wiki.db)

coeftest(mod5.did,vcov.did)

mod5.ip <- glm.nb(df,formula=m5.formula, weights=df$ip.weight)

vcov.ip <- sandwich::vcovCL(mod5.ip,df$wiki.db)

coeftest(mod5.ip,vcov.ip)

mod5.dr <- glm.nb(df, formula=m5.formula.dr, weights=df$ip.weight)

qplot(log(1+df$user_week_revert_cv))

summary(mod3_ip)

summary(mod2_dr)

mod3 <- lm(df, formula="ineq_demeaned ~ 1 + treated + week_factor + treated_with_ores")

summary(mod3)

df[,mod3.pred := predict(mod3,df)]

vcov_did <- sandwich::vcovCL(mod3_did,df[user_week_revert_cv!=0]$wiki_db)

coeftest(mod3_did,vcov_did)

ggplot(df,aes(x=weeks_from_cutoff)) + geom_point(aes(y=ineq_demeaned,color='data')) + geom_point(data=df,aes(y=mod3.pred,color='predicted')) + facet_wrap(.~never_treated) + geom_vline(data=df,xintercept=0)


mod4 <- lm(df, formula="hhi_demeaned ~ has_ores never_treated*week_factor")

summary(mod4)


