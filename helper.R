parse.date.tz <- function(s) as.POSIXct(s, format="%Y-%m-%dT%T.000Z",tz='UTC')
parse.date.iso <- function(s) as.POSIXct(s, format="%Y-%m-%d %T",tz='UTC')


equal.sample <- function(df.left, df.right){
    sample.size <- min(nrow(df.left),nrow(df.right))
    new.left <- df.left[sample(nrow(df.left),sample.size,replace=F)]
    new.right <- df.right[sample(nrow(df.right),sample.size,replace=F)]
    return(list(new.left = new.left, new.right = new.right))
}



ttr_density_plot <- function(sample, wiki, tool){
    s <- sample[ (wiki_db==wiki) & (revert_tool==tool) ]

    if(nrow(s) < 100){
        return(ggplot() + ggtitle(wiki))
    }

    s_pre <- s[pre_cutoff==T]
    s_post <- s[pre_cutoff==F]
    
    s <- rbindlist(equal.sample(s_pre,s_post))

    p2_data <- gen.ttr.density.data(s, tool)

    p <- ggplot(p2_data,aes(x=x,y=value)) + geom_line() + facet_grid(cols=vars(revert_tool), rows=vars(variable),scales='free_y')
    p <- p + scale_x_continuous(breaks = log10(duration_x_values), labels = duration_x_names) + theme(axis.text.x=element_text(angle=90,hjust=1))
    p <- p + ggtitle(wiki)
}

# adding density
# normalizing to no. sample an equal number
# try a count density

ttr_histogram_plot <- function(sample, wiki, tool, bins=50){
    s <- sample[ (wiki_db==wiki) & (revert_tool==tool) ]

    if(nrow(s) < 100){
        return(ggplot() + ggtitle(wiki))
    }

    s_pre <- s[pre_cutoff==T]
    s_post <- s[pre_cutoff==F]
    
    s <- rbindlist(equal.sample(s_pre,s_post))

    p2_data <- gen.ttr.histogram.data(s, tool, bins)
    
    p <- ggplot(p2_data,aes(xmin=xmin,xmax=xmax,ymin=0,ymax=value)) + geom_rect() + facet_grid(cols=vars(revert_tool), rows=vars(variable),scales='free_y')
    p <- p + scale_x_continuous(breaks = log10(duration_x_values), labels = duration_x_names) + theme(axis.text.x=element_text(angle=90,hjust=1))
    p <- p + ggtitle(wiki)
    return(p)
}


ttr_grid_plot <- function(sample, wiki, tool, bins=50){
    s <- sample[ (wiki_db==wiki) & (revert_tool==tool) ]

    if(nrow(s) < 100){
        return(ggplot() + ggtitle(wiki))
    }

    s_pre <- s[pre_cutoff==T]
    s_post <- s[pre_cutoff==F]
    
    s <- rbindlist(equal.sample(s_pre,s_post))

    hist.data <- gen.ttr.histogram.data(s, tool, bins)
    density.data <- gen.ttr.density.data(s, tool)

    ## scale the density so it's max is the same as the max of the histogram
    max.counts.hist <- hist.data[,.(max.hist=sort(abs(value),partial=bins-2)[bins-2]),by=.(variable)]
    max.counts.dens <- density.data[,.(max.dense=max(value)),by=.(variable)]
    scaling <- merge(max.counts.dens, max.counts.hist, by='variable')
    scaling <- scaling[,scaling.factor := max.hist / max.dense]
    density.data <- merge(density.data, scaling, by='variable',how='leftouter')
    density.data <- density.data[,value := value * scaling.factor]
    p <- ggplot(hist.data) + geom_rect(data=hist.data, mapping=aes(xmin=xmin,xmax=xmax,ymin=0,ymax=value)) + geom_line(data=density.data, mapping=aes(x=x,y=value)) + facet_grid(cols=vars(revert_tool), rows=vars(variable), scales='free_y')

    p <- p + scale_x_continuous(breaks = log10(duration_x_values), labels = duration_x_names) + theme(axis.text.x=element_text(angle=90,hjust=1))
    p <- p + ggtitle(wiki)
    return(p)
}


gen.ttr.histogram.data <- function(s, tool, bins){
    p <- ggplot(s, aes_string(x=x,y=y)) + facet_grid(as.formula(paste0(facet_row, "~", facet_col))) + geom_histogram(bins=bins) + scale_x_log10(breaks=breaks, labels=labels)

    p1_data <- data.table(ggplot_build(p)$data[[1]])
    
    p1_data_pre <- p1_data[ (PANEL == 1)]
    p1_data_post <- p1_data[ (PANEL == 2)]

    p2_data <- merge(p1_data_pre, p1_data_post, by=c('xmin','xmax'),suffixes=c(".pre",".post"))
    p2_data[,post.min.pre := count.post - count.pre]
    
    setnames(p2_data,old=c('count.pre','count.post','post.min.pre'),new = c('Before change', 'After change','After - Before'))

    p2_data <- melt(p2_data,id.vars=c('xmin','xmax'),measure.vars=c('Before change', 'After change','After - Before'))
    p2_data[,revert_tool := tool]

    return(p2_data)
}

gen.ttr.density.data <- function(s, tool){
    p <- ggplot(s, aes_string(x=x,y='..count..')) + facet_grid(as.formula(paste0(facet_row, "~", facet_col))) + geom_density() + scale_x_log10(breaks=breaks, labels=labels)

    p1_data <- data.table(ggplot_build(p)$data[[1]])
    
    p1_data_pre <- p1_data[ (PANEL == 1)]
    p1_data_post <- p1_data[ (PANEL == 2)]
    p2_data <- merge(p1_data_pre, p1_data_post, by=c('x'),suffixes=c(".pre",".post"))
    p2_data[,post.min.pre := count.post - count.pre]

    setnames(p2_data,old=c('count.pre','count.post','post.min.pre'),new = c('Before change', 'After change','After - Before'))

    p2_data <- melt(p2_data,id.vars=c('x'),measure.vars=c('Before change', 'After change','After - Before'))
    p2_data[,revert_tool := tool]

    return(p2_data)
}
