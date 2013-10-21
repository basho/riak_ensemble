DEPS_PLT=$(CURDIR)/.deps_plt
DEPS=erts kernel stdlib crypto

REBAR=./rebar

.PHONY: all compile clean deps test dialyzer typer

all: deps compile dialyzer test

clean:
	$(REBAR) clean

deps:
	$(REBAR) get-deps
	$(REBAR) compile

compile:
	$(REBAR) skip_deps=true compile

eunit: compile
	$(REBAR) skip_deps=true eunit

test: compile eunit

$(DEPS_PLT):
	@echo Building local plt at $(DEPS_PLT)
	@echo
	dialyzer --output_plt $(DEPS_PLT) --build_plt --apps $(DEPS) -r deps

dialyzer: $(DEPS_PLT)
#	dialyzer --fullpath --plt $(DEPS_PLT) -Wrace_conditions -Wunderspecs ./ebin
	dialyzer --fullpath --plt $(DEPS_PLT) -Wrace_conditions -Wunderspecs -Werror_handling -Wunmatched_returns -r ./ebin
#	dialyzer --fullpath --plt $(DEPS_PLT) -Wrace_conditions -Wunderspecs -Woverspecs -r ./ebin

typer:
	typer --plt $(DEPS_PLT) -r ./src
