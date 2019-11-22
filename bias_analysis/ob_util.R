library("logistf")
library("matrixStats")
library("data.table")
library("ggplot2")
library("lubridate")
library('forecast')
library("smooth")
library("lme4")
theme_set(theme_bw())

load_ts_ds <- function(){
    
    treated = fread("ores_bias_data/rcfilters_enabled.csv")
    treated <- treated[,.(cutoff=min(timestamp)),by='Wiki']
    treated[,cutoff := lubridate::mdy(treated$cutoff)]
    treated[,wiki_db:=Wiki]
    treated[['Wiki']] <- NULL
    df <- fread("ores_bias_data/wiki_weeks.csv",sep=',')
    df <- merge(df,treated,by=c("wiki_db"),all=TRUE)
    df <- df[,week:=lubridate::date(week)]
    df[,weeks_from_cutoff := round((week - cutoff)/dweeks(1))]
    df[,has_ores:=weeks_from_cutoff >=0]
    df[is.na(weeks_from_cutoff),has_ores := FALSE]
    df[,treated := !all(is.na(weeks_from_cutoff)),by=.(wiki_db)]
    names(df) <- sapply(names(df),function(name) gsub("_",".",name))
    return(df)
}

prepare.df <- function(df){

    ## gotta do a regex replace to fix some of the names
    namespace_names = grepl("^[01234].*",names(df))
    names(df)[namespace_names] <- sapply(names(df)[namespace_names], function(n) paste0("ns",n))

    df[,wiki.db := factor(wiki.db)]
    df[,week.factor := factor(week)]

    cutoffs <- df[treated==T,cutoff]
    get.rand.cutoff <- function(wiki){
        return(sample(cutoffs,1))
    }

    df[treated == F, cutoff := get.rand.cutoff(wiki.db),by=.(wiki.db)]

    df[treated == F, weeks.from.cutoff := round((cutoff - week)/dweeks(1))]

    ## there's a problem with n.pages.baseline

    df[is.na(view.count),view.count := 0]

    replace_regex <- function(re,df){
        for(x in names(df)[grepl(re,names(df))]){
            df[is.na(df[[x]]), (x) := 0]
        }
        return(df)
    }

    rowsum_regex <- function(re,df,newname){
        df[,(newname):=rowSums(.SD),.SDcols = names(df)[grepl(re,names(df))]]
        return(df)
    }

    reverts_regex  <-  ".*\\.?.*\\.N\\.reverts"
    df <- replace_regex(reverts_regex, df)

    df <- rowsum_regex("patroller\\..*\\.N.reverts", df, "patroller.N.reverts")
    df <- rowsum_regex("admin\\..*\\.N.reverts", df, "admin.N.reverts")
    df <- rowsum_regex("bot\\..*\\.N.reverts", df, "bot.N.reverts")    
    df <- rowsum_regex("other\\..*\\.N.reverts", df, "other.N.reverts")    


    df <- rowsum_regex(".*\\.undo\\.N.reverts", df, "undo.N.reverts")
    df <- rowsum_regex(".*\\.rollback\\.N.reverts", df, "rollback.N.reverts")
    df <- rowsum_regex(".*\\.huggle\\.N.reverts", df, "huggle.N.reverts")    
    df <- rowsum_regex(".*\\.otherTool\\.N.reverts", df, "otherTool.N.reverts")    

    reverteds_regex <- ".*\\.?.*\\.n.reverted"
    df <- replace_regex(reverteds_regex, df)

    df <- rowsum_regex("anonymous\\..*\\.n.reverted", df, "anonymous.n.reverted")
    df <- rowsum_regex("newcomer\\..*\\.n.reverted", df, "newcomer.n.reverted")
    df <- rowsum_regex("established\\..*\\.n.reverted", df, "established.n.reverted")

    edits_regex  <- ".*\\.?.*\\.N.edits"
    df <- replace_regex(edits_regex, df)
    df <- rowsum_regex(".*\\.anon.N.edits",df,"anon.N.edits")
    df <- rowsum_regex(".*\\.newcomer.N.edits",df,"newcomer.N.edits")
    df <- rowsum_regex(".*\\.non.anon.N.edits",df,"non.anon.N.edits")

    pages_regex <- "n.*\\.pages.*"
    df <- replace_regex(pages_regex, df)

    df <- df[is.na(user.week.revert.cv),user.week.revert.cv := 0]

    ttr_regex <- ".*\\..*\\.geom.mean.ttr"
    df <- replace_regex(ttr_regex, df)

    df <- rowsum_regex(".*\\.undo.geom.mean.ttr", df, "undo.geom.mean.ttr")
    df <- rowsum_regex(".*\\.rollback.geom.mean.ttr", df, "rollback.geom.mean.ttr")
    df <- rowsum_regex(".*\\.huggle.geom.mean.ttr", df, "huggle.geom.mean.ttr")
    df <- rowsum_regex(".*\\.other.tool.geom.mean.ttr", df, "other.tool.geom.mean.ttr")
    df <- rowsum_regex("anonymous\\..*\\.geom.mean.ttr", df, "anonymous.geom.mean.ttr")
    df <- rowsum_regex("newcomer\\..*\\.geom.mean.ttr", df, "newcomer.geom.mean.ttr")
    df <- rowsum_regex("established\\..*\\.geom.mean.ttr", df, "established.geom.mean.ttr")
    df <- rowsum_regex("admin\\..*\\.geom.mean.ttr", df, "admin.geom.mean.ttr")
    df <- rowsum_regex("bot\\..*\\.geom.mean.ttr", df, "bot.geom.mean.ttr")
    df <- rowsum_regex("patroller\\..*\\.geom.mean.ttr", df, "patroller.geom.mean.ttr")
    df <- rowsum_regex("other\\..*\\.geom.mean.ttr", df, "other.geom.mean.ttr")

    df[is.na(admin.geom.mean.ttr),admin.geom.mean.ttr := 0]
    df[is.na(bot.geom.mean.ttr),bot.geom.mean.ttr := 0]
    df[is.na(patroller.geom.mean.ttr),patroller.geom.mean.ttr := 0]
    df[is.na(other.geom.mean.ttr),other.geom.mean.ttr := 0]

    df[is.na(established.geom.mean.ttr),established.geom.mean.ttr := 0]
    df[is.na(newcomer.geom.mean.ttr),newcomer.geom.mean.ttr := 0]
    df[is.na(anonymous.geom.mean.ttr),anonymous.geom.mean.ttr := 0]

    df[is.na(n.pages.created.ns.4),n.pages.created.ns.4 := 0]

    df[,weekly.ns4.edits := ns4.anon.N.edits + ns4.newcomer.N.edits + ns4.non.anon.newcomer.N.edits]
    df[,":="(p.ns4.edits.anon = ns4.anon.N.edits / weekly.ns4.edits,
             p.ns4.edits.newcomer = ns4.newcomer.N.edits / weekly.ns4.edits,
             p.ns4.edits.established = ns4.non.anon.newcomer.N.edits / weekly.ns4.edits)]

    df[,ns0.edits := ns0.anon.N.edits + ns0.newcomer.N.edits + ns0.non.anon.newcomer.N.edits]
    df[,":="(p.ns0.edits.anon = ns0.anon.N.edits / ns0.edits,
             p.ns0.edits.newcomer = ns0.newcomer.N.edits / ns0.edits,
             p.ns0.edits.established = ns0.non.anon.newcomer.N.edits / ns0.edits)]

    df[, n.reverts := bot.N.reverts + admin.N.reverts + patroller.N.reverts + other.N.reverts]

    df[,":="(p.reverts.bot = bot.N.reverts / n.reverts,
             p.reverts.admin = admin.N.reverts / n.reverts,
             p.reverts.patroller = patroller.N.reverts / n.reverts,
             p.reverts.other = other.N.reverts / n.reverts )]

    df[, n.reverteds := newcomer.n.reverted + anonymous.n.reverted + established.n.reverted]

    df[,":="(p.reverteds.newcomer = newcomer.n.reverted / n.reverteds,
             p.reverteds.anonymous = anonymous.n.reverted / n.reverteds,
             p.reverteds.established = established.n.reverted / n.reverteds)]

    df[, has.patrollers := patroller.N.reverts > 0]
    df <- df[,treated.with.ores := treated & has.ores]

    ## only look at 52 weeks before and after the change
    df <- df[abs(weeks.from.cutoff) <= 52]
    return(df)
}

prepare.wikistats <- function(df){
    wiki.stats  <-  df[(weeks.from.cutoff > -52) & (week < cutoff) & (n.reverts > 0),
                       .(treated = any(treated,na.rm=T),
                         has.patrollers = any(has.patrollers, na.rm=T),
                         reverts.per.week = mean(n.reverts,na.rm=T),
                         user.revert.cv = mean(user.week.revert.cv,na.rm=T),
                         edits = mean(total.edits,na.rm=T),
                         active.editors = mean(active.editors,na.rm=T),
                         ttr = mean(geom.mean.ttr,na.rm=T),
                         admin.ttr = mean(admin.geom.mean.ttr,na.rm=T), 
                         bot.ttr = mean(bot.geom.mean.ttr,na.rm=T), 
                         admin.reverts = mean(admin.N.reverts,na.rm=T), 
                         bot.reverts = mean(bot.N.reverts,na.rm=T), #
                         patroller.ttr = mean(patroller.geom.mean.ttr,na.rm=T),
                         patroller.reverts = mean(patroller.N.reverts,na.rm=T),
                         other.ttr = mean(other.geom.mean.ttr,na.rm=T), 
                         other.reverts = mean(other.N.reverts,na.rm=T),
                         newcomer.reverted = mean(newcomer.n.reverted,na.rm=T),
                         newcomer.ttr = mean(newcomer.geom.mean.ttr,na.rm=T),
                         anon.reverted = mean(anonymous.n.reverted,na.rm=T),
                         anon.ttr = mean(anonymous.geom.mean.ttr,na.rm=T), 
                         established.reverted = mean(established.n.reverted,na.rm=T),
                         established.ttr = mean(established.geom.mean.ttr,na.rm=T),
                         n.reverteds = mean(n.reverteds, na.rm=T),
                         p.reverteds.newcomer = mean(p.reverteds.newcomer, na.rm=T),
                         p.reverteds.anonymous = mean(p.reverteds.anonymous, na.rm=T),
                         p.reverteds.established = mean(p.reverteds.established, na.rm=T),
                         ns0.anon.edits = mean(ns0.anon.N.edits,na.rm=T),
                         ns0.anon.editors = mean(ns0.anon.N.editors,na.rm=T),
                         ns0.newcomer.edits = mean(ns0.newcomer.N.edits,na.rm=T),
                         ns0.newcomer.editors = mean(ns0.newcomer.N.editors,na.rm=T),
                         ns0.established.edits = mean(ns0.non.anon.newcomer.N.edits,na.rm=T),
                         ns0.established.editors = mean(ns0.non.anon.newcomer.N.editors,na.rm=T),
                         ns1.anon.edits = mean(ns1.anon.N.edits,na.rm=T),
                         ns1.anon.editors = mean(ns1.anon.N.editors,na.rm=T),
                         ns1.newcomer.edits = mean(ns1.newcomer.N.edits,na.rm=T),
                         ns1.newcomer.editors = mean(ns1.newcomer.N.editors,na.rm=T),
                         ns1.established.edits = mean(ns1.non.anon.newcomer.N.edits,na.rm=T),
                         ns1.established.editors = mean(ns1.non.anon.newcomer.N.editors,na.rm=T),
                         ns2.anon.edits = mean(ns2.anon.N.edits,na.rm=T),
                         ns2.anon.editors = mean(ns2.anon.N.editors,na.rm=T),
                         ns2.newcomer.edits = mean(ns2.newcomer.N.edits,na.rm=T),
                         ns2.newcomer.editors = mean(ns2.newcomer.N.editors,na.rm=T),
                         ns2.established.edits = mean(ns2.non.anon.newcomer.N.edits,na.rm=T),
                         ns2.established.editors = mean(ns2.non.anon.newcomer.N.editors,na.rm=T),
                         ns3.anon.edits = mean(ns3.anon.N.edits,na.rm=T),
                         ns3.anon.editors = mean(ns3.anon.N.editors,na.rm=T),
                         ns3.newcomer.edits = mean(ns3.newcomer.N.edits,na.rm=T),
                         ns3.newcomer.editors = mean(ns3.newcomer.N.editors,na.rm=T),
                         ns3.established.edits = mean(ns3.non.anon.newcomer.N.edits,na.rm=T),
                         ns3.established.editors = mean(ns3.non.anon.newcomer.N.editors,na.rm=T),
                         ns4.edits = mean(weekly.ns4.edits, na.rm=T),
                         ns4.anon.edits = mean(ns4.anon.N.edits,na.rm=T),
                         ns4.anon.editors = mean(ns4.anon.N.editors,na.rm=T),
                         ns4.newcomer.edits = mean(ns4.newcomer.N.edits,na.rm=T),
                         ns4.newcomer.editors = mean(ns4.newcomer.N.editors,na.rm=T),
                         ns4.established.edits = mean(ns4.non.anon.newcomer.N.edits,na.rm=T),
                         ns4.established.editors = mean(ns4.non.anon.newcomer.N.editors,na.rm=T),
                         p.ns4.edits.anon = mean(p.ns4.edits.anon, na.rm=T),
                         p.ns4.edits.newcomer = mean(p.ns4.edits.newcomer, na.rm=T),
                         p.ns4.edits.established = mean(p.ns4.edits.established, na.rm=T),
                         ns0.edits = mean(ns0.edits,na.rm=T),
                         p.ns0.edits.anon = mean(p.ns0.edits.anon, na.rm=T),
                         p.ns0.edits.newcomer = mean(p.ns0.edits.newcomer, na.rm=T),
                         p.ns0.edits.established = mean(p.ns0.edits.established, na.rm=T),
                         n.reverts = mean(n.reverts, na.rm=T),
                         p.reverts.bot = mean(p.reverts.bot, na.rm=T), 
                         p.reverts.admin =  mean(p.reverts.admin, na.rm=T), 
                         p.reverts.patroller =  mean(p.reverts.patroller, na.rm=T),
                         p.reverts.other =  mean(p.reverts.other, na.rm=T),
                         view.count = mean(view.count,na.rm=T),
                         n.pages.created.ns.0 =  mean(n.pages.created.ns.0,na.rm=T),
                         n.pages.created.ns.1 =  mean(n.pages.created.ns.1,na.rm=T),
                         n.pages.created.ns.2 = mean(n.pages.created.ns.2,na.rm=T),
                         n.pages.created.ns.3 = mean(n.pages.created.ns.3,na.rm=T),
                         n.pages.created.ns.4 = mean(n.pages.created.ns.4,na.rm=T),
                         n.pages.baseline.ns.0 = mean(n.pages.baseline.ns.0,na.rm=T),
                         n.pages.baseline.ns.1 = mean(n.pages.baseline.ns.1,na.rm=T),
                         n.pages.baseline.ns.2 = mean(n.pages.baseline.ns.2,na.rm=T),
                         n.pages.baseline.ns.3 = mean(n.pages.baseline.ns.3,na.rm=T),
                         n.pages.baseline.ns.4 = mean(n.pages.baseline.ns.4,na.rm=T)
                         ),
                       by=.(wiki.db)]


    return(wiki.stats)
}

add_ip_weights <- function(df, wiki.stats, how='logistf'){
    treated.model.1.1 = logistf(data =wiki.stats, formula=treatment.formula)

    coeftest(treated.model.1.1)

    wiki.stats[,treated.probs.pred := treated.model.1.1$predict]
    wiki.stats[treated == T,ip.weight := 1/treated.probs.pred]
    wiki.stats[treated == F,ip.weight := 1/(1-treated.probs.pred)]

# add the IP-weights to the df

    df <- merge(df,wiki.stats,by=c("wiki.db"),how='left outer',suffixes=c('','.y'))
    return(list(df=df,wiki.stats=wiki.stats))
}
