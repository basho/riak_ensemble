DIALYZER_APPS=erts kernel stdlib crypto
DIALYZER_FLAGS ?= -Wunmatched_returns -Werror_handling -Wrace_conditions

include tools.mk

ifeq ($(REBAR),)
$(error "Rebar not found. Please set REBAR variable or update PATH")
endif

.PHONY: all compile clean deps test dialyzer typer

all: deps compile xref dialyzer test

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

test: testdeps compile
	bash test/run.sh

typer:
	typer --plt $(DEPS_PLT) -I include -r ./src
