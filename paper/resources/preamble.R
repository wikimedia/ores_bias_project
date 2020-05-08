library(ggplot2)
suppressMessages(library(bayesplot,quietly=TRUE))
library(data.table)
library(xtable)
library(tikzDevice)
#set xtable options
options(xtable.floating = FALSE,
        xtable.timestamp = '',
        xtable.include.rownames=FALSE,
        math.style.negative=TRUE,
        booktabs = TRUE,
        xtable.format.args=list(big.mark=','),
        xtable.sanitize.text.function=identity,
        tikzDefaultEngine='xetex'
        )
options(tinytex.verbose = TRUE)
        
base_size <- 12
theme_set(theme_bw(base_size=base_size,base_line_size=base_size/22, base_rect_size=base_size/22))

bold <- function(x) {paste('{\\textbf{',x,'}}', sep ='')}
gray <- function(x) {paste('{\\textcolor{gray}{',x,'}}', sep ='')}
wrapify <- function (x) {paste("{", x, "}", sep="")}

# load("knitr_data.RData"); now broken up into small files so we'll bring 'em all in together

f <- function (x) {formatC(x, format="d", big.mark=',')}

mcmc_areas_summary_row <- function(draws, superscript){
  draws[["tau.total"]] <- draws[[tau.1.name]] + draws[[tau.2.name]] + draws[[tau.3.name]]
  label <- bquote(sum(tau[j]^.(superscript)))
  p <- mcmc_areas(draws,pars="tau.total",prob=0.95,point_est='median') + scale_y_discrete(labels=c(label)) + theme_bw() + theme(axis.text.y = element_text(size=14))
  return(p)
}

mcmc_intervals_summary_row <- function(draws, superscript){
  draws[["tau.total"]] <- draws[[tau.1.name]] + draws[[tau.2.name]] + draws[[tau.3.name]]
  label <- bquote(sum(tau[j]^.(superscript)))
  p <- mcmc_intervals(draws,pars="tau.total",outer_prob=0.95,point_est='median') + scale_y_discrete(labels=c(label)) + theme_bw() + theme(axis.text.y = element_text(size=12))
  return(p)
}


superscript_names <- function(superscript, symbols=TRUE, tex=FALSE){
  if(tex==FALSE){
    if(symbols == TRUE)
      base_labels = c(expression(tau[1]),expression(tau[2]),expression(tau[3]))
    else
      base_labels = c("maybe bad", "likely bad", "very likely bad")

    if(!is.null(superscript)){
      labels = lapply(base_labels, function(l) bquote(.(l)^.(superscript)))
    } else {
      labels <- base_labels
    }
  } else {

    if(symbols == TRUE){
      if(!is.null(superscript)){
        labels = c("$\\tau_1^{}$","$\\tau_2^{}$","$\\tau_3^{}$")
        labels = gsub("\\{\\}",paste0('{\\\\mathrm{',superscript,'}}'),labels)
      } else {
        labels = c("$\\tau_1$","$\\tau_2$","$\\tau_3$")
      }
    } else {
      if(!is.null(superscript)){
        labels = c("maybe bad$^{}$","likely bad$^{}$","very likely bad$^{}$")
        labels = gsub("\\{\\}",paste0('{\\\\mathrm{',superscript,'}}'),labels)
      } else {
        labels = c("maybe bad","likely bad", "very likely bad")
      }
    }
  }
  return(labels)
}

my_mcmc_intervals <- function(draws, superscript=NULL,symbols=TRUE, tex=FALSE){
  labels <- rev(superscript_names(superscript,symbols=symbols,tex=tex))
  data <- mcmc_intervals_data(draws,
                              pars=c(tau.1.name,tau.2.name,tau.3.name),
                              prob_outer=0.95,
                              point_est="mean")

  p <- ggplot(data, aes(x=parameter,y=m,ymin=ll,ymax=hh)) + geom_pointrange(fatten=0.8)
  p <- p + xlab("") + ylab("Marginal Posterior") + scale_x_discrete(labels=labels,limits=rev(levels(data$parameter)))
  p <- p + theme_bw() + theme(legend.position="None", axis.text.y=element_text(color='black'),panel.grid.major=element_blank(), panel.grid.minor=element_blank())
  p <- p + geom_hline(yintercept=0, color='gray10',linetype='dashed')
  p <- p + coord_flip()
  return(p)
}

my_mcmc_areas <- function(draws, superscript=NULL){
  labels <- superscript_names(superscript)
  p <- mcmc_areas(draws,pars=c(tau.1.name,tau.2.name,tau.3.name),prob=0.95,point_est="mean") + scale_y_discrete(labels=labels) + theme_bw() + theme(axis.text.y = element_text(size=12))

  return(p)
}

big_reg_plot2 <- function(mcmc.data, group1.pattern, group1.name, group2.name, tex=FALSE){
  var.names <- names(mcmc.data)
  mcmc.data <- as.data.table(mcmc_intervals_data(mcmc.data,prob_outer=0.95,point_est="median"))
  mcmc.data[grepl('tau\\.1.*',parameter),threshold:='maybe bad']
  mcmc.data[grepl('tau\\.2.*',parameter),threshold:='likely bad']
  mcmc.data[grepl('tau\\.3.*',parameter),threshold:='very likely bad']
  mcmc.data[grepl(group1.pattern,parameter),group:=group1.name]

}

big_reg_plot <- function(mcmc.data, sup1, sup2, tex=FALSE){
  var.names <- names(mcmc.data)
  mcmc.data <- as.data.table(mcmc_intervals_data(mcmc.data,prob_outer=0.95,point_est="median"))
  mcmc.data[grepl('tau\\.1.*',parameter),threshold:='maybe bad']
  mcmc.data[grepl('tau\\.2.*',parameter),threshold:='likely bad']
  mcmc.data[grepl('tau\\.3.*',parameter),threshold:='very likely bad']

  mcmc.data[,color := c('orange','green','orange','green','orange','green','orange','green','blue')]
  mcmc.data[,linetype := c(rep('solid',6),'dotdash','dotdash','dashed')]

  if(tex == FALSE){
    display.names <- c(bquote(sum(tau[j])^.(sup1) - sum(tau[j])^.(sup2)),
                       bquote(sum(tau[j])^.(sup2)),
                       bquote(sum(tau[j])^.(sup1)),
                       rev(superscript_names(sup2)),
                       rev(superscript_names(sup1))
                       )
  } else {
    display.names <- c(paste0("$\\sum{\\tau_j^{\\mathrm{",sup1,"}}} - \\sum{\\tau_j^{\\mathrm{",sup2,"}}}$"),
                       paste0("$\\sum{\\tau_j^{\\mathrm{",sup2,"}}}$"),
                       paste0("$\\sum{\\tau_j^{\\mathrm{",sup1,"}}}$"),
                       rev(superscript_names(sup2,tex=tex)),
                       rev(superscript_names(sup1,tex=tex))
                       )
  }
  p <- ggplot(mcmc.data, aes(x=parameter,y=m, ymin=ll, ymax=hh, color=color,linetype=linetype)) + geom_pointrange(fatten=0.8) + scale_linetype_manual(values=c(solid='solid',dashed='dashed',dotted='dotted',dotdash='dotdash'))
  p <- p + xlab("") + ylab("Marginal Posterior")
  p <- p + scale_x_discrete(labels = display.names, limits=rev(levels(mcmc.data$parameter)))
  p <- p + theme_bw() + theme(legend.position="None",axis.text.y=element_text(color='black'),panel.grid.major=element_blank(), panel.grid.minor=element_blank())
  p <- p + geom_hline(yintercept=0,color='gray10',linetype='dashed')
  p <- p + coord_flip()
  return(p)
}

format.percent <- function(x) {paste(f(x*100),"\\%",sep='')}

format.day.ordinal <- function(x) {
    day <- format(x,format="%d")
    daylast <- substr(day,nchar(day),nchar(day))
    dayfirst <- substr(day,1,1)
    if(dayfirst == '0')
        day = daylast

    if( daylast == "1")
        day <- paste0(day,"st")
    else if(daylast == "2")
        day <- paste0(day,"nd")
    else if (daylast == "3")
        day <- paste0(day,"rd")
    else
        day <- paste0(day,"th")
        
    return(day)
}

format.month <- function(x){
    return( format(x,format='%B %Y'))
}

format.date <- function(x) {
    return(paste(format(x,format = '%B'),format.day.ordinal(x),format(x,format='%Y'),sep=' '))
}

format.pvalue <- function (x, digits=3) {
    threshold <- 1*10^(-1*digits)
    x <- round(x, digits)
    if (x < threshold) {
        return(paste("p<", threshold, sep=""))
    } else {
        return(paste("p=", x, sep=""))
    }
}

sparkplot <- function(samples){
  # place lines (or maybe shading?) at the mean and credible interval
  p <- qplot(samples, geom="density") + ggtitle("") + xlab("") + ylab("") + scale_y_continuous(breaks=c()) + theme_minimal() + scale_x_continuous(breaks=c())
  plot.data <- as.data.table(ggplot_build(p)[1]$data)
  ci.95 <- quantile(samples,c(0.025, 0.975))
  ci.region <- plot.data[(x>=ci.95[1]) & (x<=ci.95[2])]
  #  p <- p + geom_area(data=ci.region, aes(x=x,y=y), fill='grey',alpha=0.6)
  p <- p + geom_vline(xintercept=ci.95[1],color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=ci.95[2], color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=mean(samples), color='blue', linetype='dashed',size=3)
  p <- p + geom_vline(xintercept=0, color='black',size=3)
  return(p)
}

format.sample.stats <- function(data1,name1=NULL,data2=NULL,name2=NULL,data3=NULL,name3=NULL,data4=NULL,name4=NULL){
  threshold.names <- c("likelybad"='likely', "maybebad"='maybe', "verylikelybad"='v. likely') 
  reference.labels <- c('editor.type', "nearest.threshold","gt.nearest.threshold","N","total.weight")
  readable.labels <- c('Editors', "Threshold","Flagged","samp. N", "Weight")

  if(! is.null(name1)){
    data1[['editor.type']] <- name1
  }  else {
    reference.labels <- reference.labels[2:length(reference.labels)]
    readable.labels <- readable.labels[2:length(readable.labels)]
  }

  if(! is.null(data2)){
    data2[['editor.type']] <- name2
    df <- rbind(data1,data2)
  } else {
    df <- data1
  }
  
  # if there's a data3 assume there's a data4
  if(! is.null(data3)){
    data3[['editor.type']] <- name3
    data4[['editor.type']] <- name4
    df <- rbind(df, data3, data4)
  }

  df <- df[,":="(gt.nearest.threshold = tolower(as.character(gt.nearest.threshold)),
                 nearest.threshold=threshold.names[nearest.threshold])]

  if('editor.type' %in% reference.labels){
    df <- df[order(editor.type,nearest.threshold,gt.nearest.threshold)]
  }
  else {
    df <- df[order(nearest.threshold,gt.nearest.threshold)]
}
  setnames(df,
           old=reference.labels,
           new=readable.labels
           )

  setcolorder(df, readable.labels)

  return(df)
}


r <- readRDS("resources/remembr_hyak.RDS")
attach(r)

r2 <- readRDS("resources/notebook_remember.RDS")
attach(r2)

library(grid)
library(gridExtra)
sparkplot.files <<- list()
tau.1.name <- "nearest.thresholdmaybebad:gt.nearest.thresholdTRUE"
tau.2.name <- "nearest.thresholdlikelybad:gt.nearest.thresholdTRUE"
tau.3.name <- "nearest.thresholdverylikelybad:gt.nearest.thresholdTRUE"
cutoff.var.names <- c(
  "nearest.thresholdmaybebad:gt.nearest.thresholdTRUE",
  "nearest.thresholdlikelybad:gt.nearest.thresholdTRUE",
  "nearest.thresholdverylikelybad:gt.nearest.thresholdTRUE")

cutoff.var.symbols <- c("$\\tau_1$", "$\\tau_2$", "$\\tau_3$") 

names(cutoff.var.symbols) <- cutoff.var.names

models.draws.list <- list("mod_adoption" = mod.adoption.draws,
                          "mod_anon_reverted" = mod.anon.reverted.draws,
                          "mod_non_anon_reverted" = mod.non.anon.reverted.draws,
                          "mod_no_user_page_reverted" = mod.no.user.page.reverted.draws,
                          "mod_user_page_reverted" = mod.user.page.reverted.draws,
                          'mod_non_anon_controversial' = mod.non.anon.controversial.draws,
                          'mod_anon_controversial' =  mod.anon.controversial.draws,
                          'mod_all_controversial' = mod.all.controversial.draws,
                          'mod_no_user_page_controversial' = mod.no.user.page.controversial.draws,
                          'mod_user_page_controversial' = mod.user.page.controversial.draws)


format.regtable <- function(table.data){

  xtab <- xtable(table.data, auto=TRUE, digits=2, caption=c("Posterior statistics and percentiles for model predicting signature counts."))
                                        #align(xtab) <- xalign(xtab)
  align(xtab)['Marginal Posterior'] <- 'c'
  return(xtab)
}

plot.threshold.cutoffs <- function(df, wiki,  partial.plot=NULL){
    if(is.null(partial.plot)){
        p <- ggplot()
    } else {
        p <- partial.plot
    }

    df <- df[d.nearest.threshold != 0]
    df <- df[, ':='(pre.cutoff = d.nearest.threshold < 0)]
    
    seg.df <- df[,':='(max.x.pre.cutoff=max(.SD[(pre.cutoff==TRUE)]$d.nearest.threshold),
                      min.x.post.cutoff=min(.SD[(pre.cutoff==FALSE)]$d.nearest.threshold)),
                   by=.(nearest.threshold)]

    seg.df <- seg.df[,.(y=.SD[d.nearest.threshold==min.x.post.cutoff]$linpred,
                        yend=.SD[d.nearest.threshold==max.x.pre.cutoff]$linpred),
                     by=.(nearest.threshold)
                     ]

    
    p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,d.nearest.threshold), alpha=0.5, data=df[pre.cutoff==FALSE],  color="grey30", fill='grey30')

    p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,d.nearest.threshold), alpha=0.5, data=df[pre.cutoff==TRUE],  color="grey30", fill='grey30')
    p <- p + geom_line(aes(y=linpred, x=d.nearest.threshold), data=df[pre.cutoff==FALSE], color="grey30")

    p <- p + geom_line(aes(y=linpred, x=d.nearest.threshold), data=df[pre.cutoff==TRUE], color="grey30")

    p <- p + geom_segment(aes(x=0,xend=0,y=y,yend=yend),data=seg.df, linetype='solid', color='black')
    p <- p + scale_x_continuous(breaks = signif(c(min(df$d.nearest.threshold), 0, max(df$d.nearest.threshold)),2))
    p <- p + facet_wrap(. ~ nearest.threshold, scales="free") 
    p <- p + ggtitle(wiki) + xlab("Distance from threshold") + ylab("Prob. reverted")
    p <- p + theme(legend.position="none")
    return(p)
}

plot.bins <- function(data.plot, partial.plot = NULL){
    if (is.null(partial.plot)){
        partial.plot <- ggplot()
    }
    data.plot <- data.plot[,pre.cutoff := bin.mid <0]
    p <- partial.plot + geom_point(aes(x=bin.mid,y=prob.outcome), color="grey30", data=data.plot, alpha=0.8,size=0.8) + geom_linerange(aes(x=bin.mid,ymax=prob.outcome+1.96*se.outcome/sqrt(N),ymin=prob.outcome-1.96*se.outcome/sqrt(N)), color="grey30" ,data=data.plot, alpha=0.7, size=0.7)

    return(p)
}

rename.thresholds <- function(df){
    df <- df[nearest.threshold == 'maybebad', nearest.threshold:='maybe bad']
    df <- df[nearest.threshold == 'likelybad', nearest.threshold:='likely bad']
    df <- df[nearest.threshold == 'verylikelybad', nearest.threshold:='very likely bad']    
    df$nearest.threshold <- factor(df$nearest.threshold, levels=c('maybe bad', 'likely bad', 'very likely bad'))
    
    return(df)
}

## me.data.df.1 <- mod.anon.reverted.me.data.df
## bins.df.1 <- mod.anon.reverted.bins.df
## label.1 <- 'IP'
## me.data.df.2 <- mod.non.anon.reverted.me.data.df
## bins.df.2 <- mod.non.anon.reverted.bins.df
## label.2 <- 'Not IP'
## me.data.df.3 <- mod.no.user.page.reverted.me.data.df
## bins.df.3 <- mod.no.user.page.reverted.bins.df                     
## label.3 <- "No user page"
## me.data.df.4 <- mod.user.page.reverted.me.data.df
## bins.df.4 <- mod.user.page.reverted.bins.df
## label.4 <- "User page"

make.comparison.me.plot <- function(me.data.df.1,
                                    bins.df.1,
                                    label.1,
                                    me.data.df.2,
                                    bins.df.2,
                                    label.2,
                                    me.data.df.3,
                                    bins.df.3,
                                    label.3,
                                    me.data.df.4,
                                    bins.df.4,
                                    label.4){

    me.data.df.1 <- rename.thresholds(me.data.df.1)
    bins.df.1 <- rename.thresholds(bins.df.1)

    me.data.df.2 <- rename.thresholds(me.data.df.2)
    bins.df.2 <- rename.thresholds(bins.df.2)

    me.data.df.3 <- rename.thresholds(me.data.df.3)
    bins.df.3 <- rename.thresholds(bins.df.3)

    me.data.df.4 <- rename.thresholds(me.data.df.4)
    bins.df.4 <- rename.thresholds(bins.df.4)

    me.data.df.1 <- me.data.df.1[,label:=label.1]
    me.data.df.2 <- me.data.df.2[,label:=label.2]
    me.data.df.3 <- me.data.df.3[,label:=label.3]
    me.data.df.4 <- me.data.df.4[,label:=label.4]

    me.data.df <- rbind(me.data.df.1, me.data.df.2, me.data.df.3, me.data.df.4)
    me.data.df <- me.data.df[,label := factor(label, c(label.1, label.2, label.3, label.4))]

    me.data.df <- me.data.df[, ':='(pre.cutoff = d.nearest.threshold < 0)]

  me.data.df <- me.data.df[d.nearest.threshold != 0]
    me.data.df <- me.data.df[,nearest.threshold := factor(nearest.threshold, c('very likely bad','likely bad', 'maybe bad'))]
    
    seg.df <- me.data.df[,':='(max.x.pre.cutoff=max(.SD[(pre.cutoff==TRUE)]$d.nearest.threshold),
                               min.x.post.cutoff=min(.SD[(pre.cutoff==FALSE)]$d.nearest.threshold)),
                         by=.(label, nearest.threshold)]

    seg.df <- seg.df[,.(y=.SD[d.nearest.threshold==min.x.post.cutoff]$linpred,
                        yend=.SD[d.nearest.threshold==max.x.pre.cutoff]$linpred),
                     by=.(label, nearest.threshold)
                     ]

    bins.df.1 <- bins.df.1[,label:=label.1]
    bins.df.2 <- bins.df.2[,label:=label.2]
    bins.df.3 <- bins.df.3[,label:=label.3]
    bins.df.4 <- bins.df.4[,label:=label.4]

    bins.df <- rbind(bins.df.1, bins.df.2, bins.df.3, bins.df.4)
    bins.df <- bins.df[,label := factor(label, c(label.1, label.2, label.3, label.4))]
    bins.df <- bins.df[,nearest.threshold := factor(nearest.threshold, c('very likely bad','likely bad', 'maybe bad'))]

    bins.df <- bins.df[, ':='(pre.cutoff = bin.mid < 0)]

  plot.parts <- list()

  i <- length(plot.parts)
  first.col <- TRUE
  n.rows <- 4
  n.cols <- 3

  for(threshold in c("maybe bad","likely bad","very likely bad")){
#   xo i <- i + 1
    t.str <- paste0(toupper(substr(threshold,1,1)), substr(threshold,2,nchar(threshold)))

    #plot.parts[[i]] <- textGrob(t.str, x=unit(0.65, 'npc'))
    for(label.t in levels(me.data.df$label)){
      bins.df.t <- bins.df[(nearest.threshold == threshold) & (label==label.t)]
      me.data.df.t <- me.data.df[(nearest.threshold == threshold) & (label==label.t)]
      seg.df.t <- seg.df[(nearest.threshold == threshold) & (label==label.t)]

      p <- ggplot() + geom_point(aes(x=bin.mid,y=prob.outcome), data=bins.df.t, alpha=0.7,size=0.7, color='grey30')
      p <- p + geom_linerange(aes(x=bin.mid,ymax=prob.outcome+1.96*se.outcome/sqrt(N),ymin=prob.outcome-1.96*se.outcome/sqrt(N)),data=bins.df.t, alpha=0.7, size=0.7, color='grey30')

      p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,d.nearest.threshold, group=pre.cutoff), alpha=0.5, data=me.data.df.t[pre.cutoff==FALSE], color='grey30', fill='grey30')

      p <- p + geom_ribbon(aes(ymax=linpred.upper,ymin=linpred.lower,d.nearest.threshold, group=pre.cutoff), alpha=0.5, data=me.data.df.t[pre.cutoff==TRUE], color='grey30', fill='grey30')

      p <- p + geom_line(aes(y=linpred, x=d.nearest.threshold), color='grey30', data=me.data.df.t[pre.cutoff==FALSE])
      p <- p + geom_line(aes(y=linpred, x=d.nearest.threshold), color='grey30', data=me.data.df.t[pre.cutoff==TRUE])
      p <- p + geom_segment(aes(x=0,xend=0,y=y,yend=yend),data=seg.df.t, linetype='solid', color='black',size=0.8)
      p <- p + xlab("") + ylab("")

      ## from <- ceiling(min(me.data.df.t$linpred.lower)*100)/100
      ## print(me.data.df.t)

      p <- p + scale_y_continuous(breaks=seq(ceiling(min(me.data.df.t$linpred.lower)*100)/100, floor(max(me.data.df.t$linpred.upper)*100)/100,length.out=5))

      if( (i %% n.rows) == 0){
#        p <- p + scale_x_continuous(breaks = round(c(min(me.data.df$d.nearest.threshold), 0, max(me.data.df$d.nearest.threshold))*100)/100, t.str, position='top')
        p <- p + scale_x_continuous(breaks = c(-0.05,0,0.05), labels=c(-0.05,0,0.05),name=t.str,position='top')
      } else {
        p <- p + scale_x_continuous(breaks = signif(c(min(me.data.df$d.nearest.threshold), 0, max(me.data.df$d.nearest.threshold)),2)) + theme(axis.title.x.top = element_blank()) 
      }
      if( (i %% n.rows) == 3){
      } else {
        p <- p + theme(axis.text.x = element_blank()) + theme(axis.title.x.bottom = element_blank())
      }
#      p <- p + ggtitle(i)
      i <- i + 1
      
      plot.parts[[i]] <- p
    }
    first.col <- FALSE
  }

  y.labels <- lapply(levels(me.data.df$label), function(...) gsub("user page", "user \n page", ...))

  for(yl in y.labels){
    yh <- 0.9 - (i == 12) * 0.1
    i <- i + 1
    plot.parts[[i]] <- textGrob(yl,
                                just=c('left','top'),
                                x=unit(0.15,'grobwidth',data = ggplotGrob(plot.parts[[length(plot.parts) - n.rows]])),
                                y=unit(yh,'npc'),
                                gp=gpar(fontsize=11))
  }

  p.main <- arrangeGrob(grobs = plot.parts, as.table = FALSE, ncol=4, widths=c(rep(1,3),0.5), heights=c(1.15,1,1,1.1))

  return(grid.arrange(textGrob("Prob. reverted",rot=90,x=0.6), p.main,  textGrob(""), textGrob("Distance from threshold",x=0.455,y=0.7, just=c('bottom')), ncol=2, widths=c(0.02,1), heights=c(1,0.018)))
}

make.rdd.plot <- function(me.data.df, bins.df, title){
    me.data.df <- rename.thresholds(me.data.df)
    bins.df <- rename.thresholds(bins.df)

    p <- plot.bins(bins.df)

    p <-  plot.threshold.cutoffs(me.data.df, '', partial.plot = p)

    p <- p + ggtitle(title)

    p <- p + theme(panel.spacing = unit(2, "lines"), plot.title = element_text(size=12))
    return(p)
}


prep.regtable <- function(mod.xtable, name){

  table.data <- as.data.table(mod.xtable)
  table.data <- table.data[varname %in% cutoff.var.names]
  tex.names <- cutoff.var.symbols
  table.data[['varname']] = cutoff.var.symbols[table.data$varname]
  table.data <- table.data[order(varname)]

  for(i in 1:length(cutoff.var.symbols)){
    var <- cutoff.var.symbols[[i]]
    table.data[varname == var,"Marginal Posterior":= paste0("\\raisebox{-0.5\\totalheight}{\\includegraphics[height=1.4em]{",sparkplot.files[[paste(name,var,sep='.')]],"}}")]
  }
  
#table.data[,Rhat:=NULL]

  setnames(table.data,old=c("varname","mean","sd", "2.5%","25%","50%","75%","97.5%", "Rhat"),new=c("Coefficient", "Mean", "SD", "2.5\\%","25\\%","50\\%","75\\%","97.5\\%", "\\(\\widehat{R}\\)"))

  xtab <- format.regtable(table.data)
  return(xtab)
}

get.CI.str <- function(draws, beta = 0.95, digits=2, transform.f = identity){
  t.lower <- (1 - beta)/2
  t.upper <- 1 - t.lower
  q <- quantile(draws, c(t.lower, t.upper))
  q <- transform.f(q)
  q <- signif(q, digits)
  return(paste0('(',q[1],', ',q[2],')'))
}

proto.reverted.CI.str <- function(proto.data,digits=2){
  return(paste0("$",
                signif(proto.data$linpred,digits=digits),
                "~(95\\%~\\mathrm{CI}=(",
                signif(proto.data$linpred.lower, digits=digits),
                ",~",
                signif(proto.data$linpred.upper, digits=digits),
                ")$)"))
}


proto.reverted <- function(data.df, where='below', threshold='very likely bad'){
  if (where=='below'){
    x <- data.df[ (nearest.threshold==threshold) & (d.nearest.threshold < 0), .(max(d.nearest.threshold))]
  } else {
    x <- data.df[ (nearest.threshold==threshold) & (d.nearest.threshold > 0), .(min(d.nearest.threshold))]
  }
  r <- data.df[(nearest.threshold==threshold) & (d.nearest.threshold == x$V1),
                 .(linpred.lower,linpred.upper,linpred,d.nearest.threshold)]
  return(r)
}

plot.marginal.posterior <- function(samples, name, var){
  p <- qplot(samples, geom="density") + ggtitle("") + xlab("") + ylab("") + scale_y_continuous(breaks=c(0)) + theme_minimal() 
  print(p)
}

sparkplot <- function(samples){
  # place lines (or maybe shading?) at the mean and credible interval
  p <- qplot(samples, geom="density") + ggtitle("") + xlab("") + ylab("") + scale_y_continuous(breaks=c()) + theme_minimal() 
  plot.data <- as.data.table(ggplot_build(p)[1]$data)
  ci.95 <- quantile(samples,c(0.025, 0.975))
  ci.region <- plot.data[(x>=ci.95[1]) & (x<=ci.95[2])]
  x.min <- min(c(plot.data$x, 0))
  x.max <- max(c(plot.data$x, 0))
  
  #  p <- p + geom_area(data=ci.region, aes(x=x,y=y), fill='grey30',alpha=0.6)
  p <- p + geom_vline(xintercept=ci.95[1],color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=ci.95[2], color='purple',size=5,linetype='dotted')
  p <- p + geom_vline(xintercept=mean(samples), color='blue', linetype='dashed',size=3)
  p <- p + geom_vline(xintercept=0, color='black',size=3)
  breaks <- signif(c(x.min,x.max),2)
  p <- p + scale_x_continuous(breaks=breaks, labels=as.character(breaks), limits=c(x.min, x.max))
  ## panel.grid.major.x = element_blank(),
  p <- p + theme(plot.margin=unit(c(0,10,0,10),'mm'),  axis.text.x=element_text(size=56))
  return(p)
}

make.sparkplot <- function(samples, name, var){
    fname <- paste0("figures/",name,'_',gsub('\\.','_',var),".pdf")
    fname <- gsub('\\\\','',fname)
    fname <- gsub('\\$','',fname)

    sparkplot.name <- paste(name,var,sep='.')
    sparkplot.files[[sparkplot.name]] <<- fname

    if( (overwrite == TRUE) | (!file.exists(fname))){
      p <- sparkplot(samples) 
      cairo_pdf(fname,width=10,height=2.6)
      print(p)
      dev.off()
      system2(command = "pdfcrop", 
              args    = c(fname, 
                          fname) 
              )

      ## system2(command = "gs", 
      ##         args    = c('-o',
      ##                     fname,
      ##                     '-sDevice=pdfwrite',
      ##                     '-dColorConversionStrategy=/sRGB',
      ##                     '-dProcessColorModel=/DeviceRGB',
      ##                     fname) 
      ##         )
    }
    return(sparkplot.name)
}

make.overall.regtab.row <- function(samples, name, coef.name){

  quant <- quantile(samples,probs=c(2.5,25,50,75,97.5)/100)
  names(quant) <- c('2.5\\%','25\\%','50\\%','75\\%','97.5\\%')

  sparkplot.name <- make.sparkplot(samples,name,'overall')

  row <- list('Coefficient'=coef.name,
                       'Mean'=mean(samples),
                       'SD'=sd(samples),
                       "\\(\\widehat{R}\\)"=NA,
                       'Marginal Posterior'=paste0("\\raisebox{-0.4\\totalheight}{\\includegraphics[height=1.4em]{",sparkplot.files[[sparkplot.name]],"}}")
)

  return (append(row, quant))
}

make.sparklines <- function(model.draws, name){
  for(i in 1:length(cutoff.var.names)){
    model.draws[,cutoff.var.symbols[i]:=cutoff.var.names[i]]
    }
  draws <- model.draws

  for (var in cutoff.var.symbols){
    make.sparkplot(draws[[var]], name, var)
  }
  
}


## h1.mcmc.data[,display.name:=display.name.map[variable]]

## ggplot(h1.mcmc.data, aes(x=value, y=variable, color=color)) + geom_density() + scale_y_discrete(labels=display.name.map)

## xlims <- c(min(h1.mcmc.data$value),max(h1.mcmc.data$value))
               
## color_scheme_set("orange")
## p1 <- my_mcmc_intervals(mod.anon.reverted.draws,superscript='IP') 
## p2 <- mcmc_intervals_summary_row(mod.anon.reverted.draws,superscript='IP')
## color_scheme_set("blue")
## p3 <- my_mcmc_intervals(mod.non.anon.reverted.draws,superscript='not IP') 
## p4 <- mcmc_intervals_summary_row(mod.non.anon.reverted.draws,superscript='not IP')
## color_scheme_set("green")
## p5 <- mcmc_intervals(data.table(tau.non.anon.sub.anon)) + scale_y_discrete(labels=c(expression(sum(tau[j]^"not IP") - sum(tau[j]^"IP")))) + theme(axis.text.y = element_text(size=12))
## ## p <- bayesplot_grid(p1,p2,p3,p4,p5,grid_args=list("layout_matrix"=rbind(c(1,1,2,2),
## ##                                                                         c(1,1,2,2),
## ##                                                                         c(3,3,4,4),
## ##                                                                         c(3,3,4,4),
## ##                                                                         c(6,5,5,6),
## ##                                                                         c(6,5,5,6))))
## p <- bayesplot_grid(p1,p2,p3,p4,p5,xlim = c(-1.2,2.5),grid_args=list("ncol"=1))
## print(p)
## h1.mcmc.data <- as.data.table(mcmc_intervals_data(h1.mcmc.data,prob_outer=0.95,point_est="median"))

## color.map <- list(tau.1.anon='green',
##                   tau.2.anon='green',
##                   tau.3.anon='green',
##                   tau.anon='green',
##                   tau.1.non.anon='orange',
##                   tau.2.non.anon='orange',
##                   tau.3.non.anon='orange',
##                   tau.non.anon='orange',
##                   tau.non.anon.sub.anon='blue')


## color.map <- data.table(color.map,parameter=names(color.map))
## h1.mcmc.data <- h1.mcmc.data[color.map,color:=color.map,on=.(parameter)]

## linetype.map <- c(tau.1.anon='solid',
##                   tau.2.anon='solid',
##                   tau.3.anon='solid',
##                   tau.anon='dotdash',
##                   tau.1.non.anon='solid',
##                   tau.2.non.anon='solid',
##                   tau.3.non.anon='solid',
##                   tau.non.anon='dotdash',
##                   tau.non.anon.sub.anon='dashed')

## linetype.map <- data.table(linetype.map, parameter = names(linetype.map))
## h1.mcmc.data <- h1.mcmc.data[linetype.map, linetype:=linetype.map, on=.(parameter)]

## display.name.map <- c(tau.1.anon=expression(tau[1]^'IP'),
##                       tau.2.anon=expression(tau[2]^'IP'),
##                       tau.3.anon=expression(tau[3]^'IP'),
##                       tau.anon=expression(sum(tau[j])^'IP'),
##                       tau.1.non.anon=expression(tau[1]^'not IP'),
##                       tau.2.non.anon=expression(tau[2]^'not IP'),
##                       tau.3.non.anon=expression(tau[3]^'not IP'),
##                       tau.non.anon=expression(sum(tau[j])^'not IP'),
##                       tau.non.anon.sub.anon=expression(sum(tau[j])^'not IP' - sum(tau[j])^'IP'))

## p <- ggplot(h1.mcmc.data, aes(x=parameter,y=m, ymin=ll, ymax=hh, color=color,linetype=linetype)) + geom_pointrange() + scale_linetype_manual(values=c(solid='solid',dashed='dashed',dotted='dotted',dotdash='dotdash'))
## p <- p + xlab("") + ylab("Marginal Posterior") + scale_x_discrete(labels = display.name.map, limits=rev(levels(h1.mcmc.data$parameter))) + theme_minimal() + theme(legend.position="None") 
## p <- p + geom_hline(yintercept=0,color='gray30')
## p <- p + coord_flip()
## p
