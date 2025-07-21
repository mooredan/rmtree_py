
.PHONY: all
all : devel.log

dump.log : $(wildcard *.py)
	python3 dump_place_table.py  > dump.log


devel.log : $(wildcard *.py)
	python3 dump_place_table.py  > places-before.log
	python3 devel.py | tee devel.log
	python3 dump_place_table.py  > places-after.log


.PHONY: fuzzy
fuzzy : dump.log
	python3 place_fuzzy_match.py | tee fuzzy.log 


.PHONY: clean
clean :	
	cd ../Genealogy && git restore ZebMoore_Ancestry.rmtree
	@ rm -f dump.log
	@ rm -f devel.log
	@ rm -f left.log
	@ rm -f fuzzy.log
	@ rm -f *.log
