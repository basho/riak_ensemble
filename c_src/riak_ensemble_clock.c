/********************************************************************
 *
 *  Copyright (c) 2014 Basho Technologies, Inc. All Rights Reserved.
 *
 *  This file is provided to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License.  You may obtaine
 *  a copy of the License at
 *
 *    http:  www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 ********************************************************************/
#include "erl_nif.h"

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>

#if defined(__MACH__) && defined(__APPLE__)
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;

#if defined(__MACH__) && defined(__APPLE__)
static mach_timebase_info_data_t timebase_info;
#endif

/*********************************************************************/

#if defined(_POSIX_TIMERS) && (_POSIX_TIMERS > 0)
uint64_t posix_get_clock(clockid_t clock)
{
  struct timespec ts;
  if(clock_gettime(clock, &ts) == -1)
    return 0;
  return ((uint64_t)ts.tv_sec * 1000000000) + ts.tv_nsec;
}

/* Note: Prefer CLOCK_BOOTTIME on Linux where supported, as this
         includes time spent in suspend. CLOCK_MONOTONIC may or may
         not include time spent in suspend -- it's CPU dependent. In
         practice, this shouldn't matter -- people don't typically
         suspend/resume production servers while under client load.
         Likewise, client TCP connections are unlikely to survive
         across reasonable suspend durations.
*/

uint64_t posix_monotonic_time(void)
{
  uint64_t time;
#if defined(CLOCK_BOOTTIME)
  if((time = posix_get_clock(CLOCK_BOOTTIME)))
    return time;
#elif defined(CLOCK_MONOTONIC)
  if((time = posix_get_clock(CLOCK_MONOTONIC)))
    return time;
#endif
  return 0;
}
#endif

/*********************************************************************
 * See Apple technical note:                                         *
 *   https://developer.apple.com/library/mac/qa/qa1398/_index.html   *
 *********************************************************************/

/* Note: mach_absolute_time() is based on the CPU timestamp counter,
         which is synchronized across all CPUs since Intel Nehalem.
         Earlier CPUs do not provide this guarantee. It's unclear if
         Apple provides any correction for this behavior on older CPUs.
         We assume this doesn't matter in practice -- people don't use
         ancient OS X machines as production servers.
*/

#if defined(__MACH__) && defined(__APPLE__)
uint64_t osx_monotonic_time(void)
{
  uint64_t time;
  uint64_t timeNano;

  time = mach_absolute_time();

  // Do the maths. We hope that the multiplication doesn't 
  // overflow; the price you pay for working in fixed point.

  timeNano = time * timebase_info.numer / timebase_info.denom;

  return timeNano;
}
#endif

/*********************************************************************/

static uint64_t get_monotonic_time()
{
  uint64_t time = 0;

#if defined(__MACH__) && defined(__APPLE__)
  time = osx_monotonic_time();
#endif

#if defined(_POSIX_TIMERS) && (_POSIX_TIMERS > 0)
  time = posix_monotonic_time();
#endif

  return time;
}

/*********************************************************************/

static ERL_NIF_TERM monotonic_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  uint64_t time = get_monotonic_time();

  if(time) {
    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, time));
  }
  else {
    return ATOM_ERROR;
  }
}

/*********************************************************************/

static ERL_NIF_TERM monotonic_time_ms(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  uint64_t time = get_monotonic_time() / 1000000;

  if(time) {
    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, time));
  }
  else {
    return ATOM_ERROR;
  }
}

/*********************************************************************/

static void init(ErlNifEnv *env)
{
  ATOM_OK = enif_make_atom(env, "ok");
  ATOM_ERROR = enif_make_atom(env, "error");

#if defined(__MACH__) && defined(__APPLE__)
  (void) mach_timebase_info(&timebase_info);
#endif
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
  init(env);
  return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data,
                      ERL_NIF_TERM load_info)
{
  init(env);
  return 0;
}

static void on_unload(ErlNifEnv *env, void *priv_data)
{
}

/*********************************************************************/

static ErlNifFunc nif_funcs[] = {
  {"monotonic_time", 0, monotonic_time},
  {"monotonic_time_ms", 0, monotonic_time_ms}
};

ERL_NIF_INIT(riak_ensemble_clock, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload)
