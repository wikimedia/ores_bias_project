#!/usr/bin/make
all: notes.config vc $(patsubst %.Rtex,%.pdf,$(wildcard *.Rtex)) 

refs.bib:
	wget -r -q -O refs.bib http://127.0.0.1:23119/better-bibtex/collection?/2/QZE3X7B3.biblatex


%.tex: %.Rtex refs.bib
	Rscript -e "library(knitr); knit('$<')"


%.pdf: %.tex 
	latexmk -f -pdf -xelatex $<

notes.config:
	echo '\setbeameroption{show notes on second screen}' > notes.config

# rules for generating notesonly
notesonly-config:
	echo '\setbeameroption{show only notes}' > notes.config
notesonly: notesonly-config pdf

# rules for generating slides only
slides-config:
	echo '' > notes.config
slides: slides-config pdf

clean: 
	latexmk -C *.tex
	rubber -d --clean *.tex
	rm -f *.tex
	rm -f *.bbl
	rm -f *.run.xml
	rm -f vc
	rm -f *.tmp
	rm -f vc
	rm -f notes.config
	rm -f *.pdfpc

viewpdf: all
	evince *.pdf

spell:
	aspell -c -t --tex-check-comments -b text.tex

vc:
	resources/vc-git

pdf: all

.PHONY: clean all notesonly-config notesonly slides-config slides viewpdf pdfpc spell
.PRECIOUS: %.tex
