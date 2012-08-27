PGSHOME=/home/kikht/opt/sdptk
PGSINC=$(PGSHOME)/include
PGSLIB=$(PGSHOME)/lib/linux

l0merge_modis: l0merge_modis.c
	gcc -O3 -Wall -pedantic -ansi -o $@ -I$(PGSINC) -L$(PGSLIB) $^ -lPGSTK -lm

debug: l0merge_modis.c
	gcc -O0 -g -Wall -pedantic -ansi -o l0merge_modis -I$(PGSINC) -L$(PGSLIB) $^ -lPGSTK -lm	

clean:
	rm -rf l0merge_modis
