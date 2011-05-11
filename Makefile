OCAMLMAKEFILE = OCamlMakefile

SOURCES = fSTypes2.ml castle_c.c castle.ml
RESULT  = castle
THREADS = yes
CLIBS = castle
PACKS = 

LIBINSTALL_FILES = \
	fSTypes2.cmi \
	fSTypes2.cmx \
	castle.cmi \
	castle.cmx \
	castle.cma \
	castle.a \
	castle.cmxa \
	libcastle_stubs.a \
	dllcastle_stubs.so

OCAMLFLAGS=-g -w Aez -warn-error Aez
OCAMLLDFLAGS=-g
CFLAGS = -Wall -Werror -g -ggdb -O2 $(RPM_OPT_FLAGS)

# XXX: This is in order to install the lib into the correct /tmp dir when
# rpmbuild-ing. This might be broken now, test and fix(?)
ifdef DISTLIBDIR
	OCAMLDISTLIBDIR=$(DISTLIBDIR)/ocaml
    OCAMLFIND_INSTFLAGS = -destdir $(OCAMLDISTLIBDIR)
endif

all: byte-code-library native-code-library

cleanlibs:
	ocamlfind remove $(RESULT)
	if [ "$(OCAMLDISTLIBDIR)" != "" ]; then mkdir -p $(OCAMLDISTLIBDIR); fi

install: cleanlibs libinstall

include $(OCAMLMAKEFILE)
