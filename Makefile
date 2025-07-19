
.PHONY: all
all : left

dump.log : $(wildcard *.py)
	cd ../Genealogy && git restore ZebMoore_Ancestry.rmtree
	python3 devel.py
	python3 dump_place_table.py  > dump.log


.PHONY: fuzzy
fuzzy : dump.log
	python3 place_fuzzy_match.py | tee fuzzy.log 


.PHONY: left
left : dump.log
	@ grep -v -e ', USA' \
	        -e ', Canada' \
	        -e ', Italy' \
	        -e ', Ireland' \
	        -e ', England' \
	        -e ', Germany' \
	        -e ', Thailand' \
	        -e ', Philippines' \
	        -e ', Switzerland' \
	        -e ', Mexico' \
	        -e ', Honduras' \
	        -e ', Japan' \
	        -e ', Scotland' \
	        -e ', China' \
	        -e ', Ghana' \
	        -e ', Ivory Coast' \
	        -e ', Peru' \
	        -e ', Columbia' \
	        -e ', Chile' \
	        -e ', Boliva' \
	        -e ', Brazil' \
	        -e ', Nigeria' \
	        -e ', Samoa' \
	        -e ', Paraguay' \
	        -e ', Argentina' \
	        -e ', Greece' \
	        -e ', Australia' \
	        -e ', Venzuela' \
	        -e ', Venezuela' \
	        -e ', Bolivia' \
	        -e ', Denmark' \
	        -e ', Ecuador' \
	        -e ', Argentina' \
	        -e ', Uruguay' \
	        -e ', Kenya' \
	        -e ', Tonga' \
	        -e ', Finland' \
	        -e ', New Zealand' \
	        -e ', Netherlands' \
	        -e ', Guatemala' \
	        -e ', South Africa' \
	        -e ', Ukraine' \
	        -e ', Portugal' \
	        -e ', Sweden' \
	        -e '^Sweden$$' \
	        -e '^USA$$' \
	        -e '^Ireland$$' \
	        -e '^India$$' \
	        -e '^Nigeria$$' \
	        -e '^Samoa$$' \
	        -e '^Argentina$$' \
	        -e '^Denmark$$' \
	        -e '^Ecuador$$' \
	        -e '^Italy$$' \
	        -e '^Bermuda$$' \
	        -e '^Australia$$' \
	        -e '^Finland$$' \
	        -e '^Kenya$$' \
	        -e '^France$$' \
	        -e '^Haiti$$' \
	        -e '^El Salvador$$' \
	        -e '^Sweden$$' \
	        -e '^England$$' \
	        -e '^Norway$$' \
	        -e '^France$$' \
	        -e '^Germany$$' \
	        -e '^Ireland$$' \
	        -e '^Switzerland$$' \
	        -e '^Italy$$' \
	        -e '^Bermuda$$' \
	        -e '^Mexico$$' \
	        -e '^Lebanon$$' \
	        -e '^Denmark$$' \
	        -e '^Venzuela$$' \
	        -e '^Venezuela$$' \
	        -e '^Norway$$' \
                dump.log > left.log

.PHONY: clean
clean :	
	rm -f dump.log
	rm -f left.log
	rm -f fuzzy.log
