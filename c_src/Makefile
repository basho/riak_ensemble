# -------------------------------------------------------------------
#
# Copyright (c) 2017 Basho Technologies, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------
#
# Requires GNU Make functionality and a Unix-y environment.
#

prj_dir	:= $(abspath $(CURDIR)/..)
project	:= $(strip $(notdir $(prj_dir)))

module	:= riak_ensemble
version	:= $(shell git describe --tags 2>/dev/null | cut -d- -f1)

# probably overkill, but we really want the system's POSIX uname and not
# any wrapper/alternate that may come first in the path
uname	:= $(or $(wildcard /usr/bin/uname), \
		$(wildcard /bin/uname), \
		$(shell which uname 2>/dev/null), \
		$(error Can't find acceptable uname program))

# match the 'Osname' returned by erlang os:type()
sys_type := $(shell $(uname) -s | tr '[A-Z]' '[a-z]')
# it's unlikely there's any case variation, but make sure
sys_arch := $(shell $(uname) -m | tr '[A-Z]' '[a-z]')
sys_rel  := $(shell $(uname) -r | tr '[A-Z]' '[a-z]')

dist_dir := $(prj_dir)/priv

ifneq ($(ERLANG_ROOT_DIR),)
erts_inc := $(ERLANG_ROOT_DIR)/usr/include
else
erl_exe	:= $(or $(if $(ERL),$(shell which $(ERL) 2>/dev/null)), \
		$(shell which erl 2>/dev/null), \
		$(error Can't find Erlang/OTP runtime))
erts_inc := $(shell $(erl_exe) -noshell -eval \
		'io:put_chars(code:root_dir()), erlang:halt().' \
		2>/dev/null)/usr/include
endif

# directory where intermediate artifacts will go
ifneq ($(REBAR_BUILD_DIR),)
work_dir    := $(REBAR_BUILD_DIR)/lib/$(module)/_work
else
work_dir    := $(prj_dir)/_work
endif
work_dir    := $(work_dir)/$(sys_type)-$(sys_arch)-$(sys_rel)

# base name of the NIF library that will be calculated by the loader
so_name	:= $(module)_$(sys_type)_$(sys_arch)
# where it's going to end up when we're done, this is the default target
so_dest	:= $(dist_dir)/$(so_name).so

# The NIF is pretty straightforward
nif_srcs := $(wildcard $(prj_dir)/c_src/*.c)
nif_objs := $(foreach n, $(sort $(basename $(notdir \
		$(nif_srcs)))), $(work_dir)/$(n).o)

o_level	:= -g2 -O3

LDFLAGS := -m64
CFLAGS	:= $(LDFLAGS)

so_flag	:= -shared

ecp	:= /bin/cp -p
# build tools
ifeq	($(sys_type),darwin)
ecp	:= /bin/cp -pX
CC	:= /usr/bin/cc
LDFLAGS := -arch x86_64
LDFLAGS	+= $(shell test $(firstword $(subst ., ,$(sys_rel))) -lt 14 \
		|| echo '-mmacosx-version-min=10.9')
CFLAGS	:= $(LDFLAGS)
so_flag	:= -flat_namespace -undefined suppress -dynamiclib
else ifeq ($(sys_type),freebsd)
CFLAGS	+= _REENTRANT
CC	:= $(shell which cc)
else ifeq ($(sys_type),linux)
CC	:= $(shell which gcc)
else ifeq ($(sys_type),sunos)
CFLAGS	+= _REENTRANT
CC	:= $(shell which gcc)
else
$(error Unsupported platform '$(sys_type)')
endif
CFLAGS    += -std=c99 $(o_level) -fPIC -Wall -Wmissing-prototypes
LDFLAGS   += $(o_level)
CPPFLAGS  := -I$(erts_inc)

.PHONY:	default dist clean clean-dist veryclean

# don't automatically clean up intermediates
.SECONDARY:

default :: dist

dist :: $(so_dest)

clean ::
	/bin/rm -rf $(work_dir)

clean-dist :: clean
	/bin/rm -f $(so_dest)

veryclean :: clean-dist

$(dist_dir)/$(so_name).so : $(work_dir)/$(so_name).so
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(ecp) $< $@

$(work_dir)/$(so_name).so : $(nif_objs)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(strip $(CC) $(so_flag) $(LDFLAGS) -o $@ $^)

$(work_dir)/%.o : $(prj_dir)/c_src/%.c
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(strip $(CC) -c $(CFLAGS) $(CPPFLAGS) -o $@ $<)
