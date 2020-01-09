#!/usr/bin/make
all: $(patsubst %.Rtex,%.pdf,$(wildcard *.Rtex)) 

refs.bib:
	wget -r -q -O OresAudit.bib http://127.0.0.1:23119/better-bibtex/collection?/4/OresAudit.biblatex

copy.to.dropbox:
	cp OresAudit.bib dropbox
	cp *.RDS dropbox

%.tex: %.Rtex refs.bib
	Rscript -e "library(knitr); knit('$<')"


%.pdf: %.tex
	latexmk -f -pdf $<

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

.PHONY: clean all refs.bib copy.to.dropbox
.PRECIOUS: %.tex
