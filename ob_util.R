library("data.table")
library("ggplot2")
library("lubridate")
library('forecast')
library("smooth")
library("lme4")
theme_set(theme_bw())

load_ts_ds = function(){

    treated = fread("ores_bias_data/rcfilters_enabled.csv")
    treated <- treated[,.(cutoff=min(timestamp)),by='Wiki']
    treated[,cutoff := lubridate::mdy(treated$cutoff)]
    treated[,wiki_db:=Wiki]
    treated[['Wiki']] <- NULL
    df <- fread("ores_bias_data/wiki_weeks.csv",sep=',')
    df <- merge(df,treated,by=c("wiki_db"),all=TRUE)
    df <- df[,week:=lubridate::ymd(week)]
    df[,weeks_from_cutoff := round((week - cutoff)/dweeks(1))]
    df[,has_ores:=weeks_from_cutoff >=0]
    df[is.na(weeks_from_cutoff),has_ores := FALSE]
    df[,treated := !all(is.na(weeks_from_cutoff)),by=.(wiki_db)]
    return(df)
}