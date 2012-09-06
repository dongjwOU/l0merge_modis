# PGSHOME=/home/kikht/opt/sdptk-lto
# PGSINC=$(PGSHOME)/include
# PGSLIB=$(PGSHOME)/lib/linux

CFLAGS_BASE=$(CFLAGS) -Wall -pedantic -ansi
CFLAGS_RELEASE=$(CFLAGS_BASE) -O3 -fwhole-program -flto -fuse-linker-plugin
CFLAGS_DEBUG=$(CFLAGS_BASE) -O0 -g

ifdef PGSHOME
	PGSINC?=$(PGSHOME)/include
	PGSLIB?=$(PGSHOME)/lib/linux
	PGSFLAGS=-I$(PGSINC) -L$(PGSLIB) -lPGSTK -DHAVE_SDPTOOLKIT
endif

l0merge_modis: l0merge_modis.c
ifndef PGSFLAGS
	echo "Building without SDPToolkit!"
endif
	$(CC) $(CFLAGS_RELEASE) -o $@ $^ $(PGSFLAGS) -lm

l0merge_modis_debug: l0merge_modis.c
	$(CC) $(CFLAGS_DEBUG) -o $@ $^ $(PGSFLAGS) -lm

clean:
	rm -rf l0merge_modis l0merge_modis_debug
