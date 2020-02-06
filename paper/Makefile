#!/usr/bin/make
all: $(patsubst %.Rtex,%.pdf,$(wildcard *.Rtex)) 

refs.bib:
	wget -r -q -O OresAudit.bib http://127.0.0.1:23119/better-bibtex/collection?/3/OresAudit.bibtex

sync: sync.remember sync.figures

sync.remember:
	scp hyak:/gscratch/comdata/users/nathante/ores_bias_project/remembr.RDS remembr_hyak.RDS
	cp remembr_hyak.RDS dropbox/resources
	mv remembr_hyak.RDS resources
	scp notebook1004.eqiad.wmnet:/srv/home/nathante/ores_bias_project/remember_dict.pickle notebook_remember_dict.pickle
	python3 ../convert_remember_dict.py notebook_remember_dict.pickle --output resources/notebook_remember.RDS
	rm notebook_remember_dict.pickle
	cp resources/notebook_remember.RDS dropbox/resources

sync.figures:
	scp hyak:/gscratch/comdata/users/nathante/ores_bias_project/plots/user_page_controversial_plot.png .
	cp user_page_controversial_plot.png figures
	mv user_page_controversial_plot.png figures

copy.to.dropbox:
	cp OresAudit.bib dropbox
	cp *.RDS dropbox
	cp figures/* dropbox/figures/

%.tex: %.Rtex refs.bib
	Rscript -e "library(knitr); knit('$<')"


%.pdf: %.tex
	latexmk -xelatex -f -pdf $<

clean: 
	latexmk -C *.tex
	rm -f *.tex
	rm -f *.bbl
	rm -f *.run.xml

viewpdf: all
	evince *.pdf

spell:
	aspell -c -t --tex-check-comments -b text.tex

pdf: all

.PHONY: clean all refs.bib copy.to.dropbox sync.remember sync.figures
.PRECIOUS: %.tex
