DIALYZER_APPS=erts kernel stdlib crypto
DIALYZER_FLAGS ?= -Wunmatched_returns -Werror_handling -Wrace_conditions

.PHONY: all compile clean deps test dialyzer typer

all: deps compile

clean:
	$(REBAR) clean

deps:
	$(REBAR) get-deps
	$(REBAR) compile

compile:
	$(REBAR) skip_deps=true compile

testdeps: deps
	$(REBAR) -C rebar.test.config get-deps
	$(REBAR) -C rebar.test.config compile

runtests: testdeps compile
	bash test/run.sh

update-doc-lines:
	@escript doc/update_line_numbers.erl ebin doc/*.md

typer:
	typer --plt $(DEPS_PLT) -I include -r ./src

include tools.mk

ifeq ($(REBAR),)
$(error "Rebar not found. Please set REBAR variable or update PATH")
endif

## Override test after tools.mk; use custom test runner for isolation.
test: testdeps
	bash test/run.sh
