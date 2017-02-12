/********************************************************************
 *
 *  Copyright (c) 2014-2017 Basho Technologies, Inc.
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

/* this forces C99-like datatypes */
#include <stdint.h>

#if defined(__MACH__) && defined(__APPLE__)
#include <mach/mach.h>
#include <mach/mach_time.h>
/*
    mach_absolute_time() is based on the CPU's TSC, which is synchronized
    across all CPUs/cores since Intel Nehalem. Earlier CPUs do not provide
    this guarantee, and it's unclear if Apple provides any correction for
    this behavior on older CPUs.
    We assume this doesn't matter in practice -- people don't use ancient
    OS X machines as production servers.
*/
#define Use_Mach_Time 1

#else   /* POSIX-y */
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#if     defined(_POSIX_TIMERS) && (_POSIX_TIMERS > 0) \
    &&  (defined(CLOCK_BOOTTIME) || defined(CLOCK_MONOTONIC))
/*
    Prefer CLOCK_BOOTTIME on Linux where supported, as this includes time
    spent in suspend. CLOCK_MONOTONIC and CLOCK_MONOTONIC_RAW may or may not
    include time spent in suspend -- it's CPU dependent.
    In practice, this shouldn't matter -- people don't typically
    suspend/resume production servers while under client load.
    Likewise, client TCP connections are unlikely to survive across
    realistic suspend durations.
*/
#if     defined(CLOCK_BOOTTIME)
#define Use_Posix_Clock CLOCK_BOOTTIME
#elif   defined(CLOCK_MONOTONIC_RAW)
#define Use_Posix_Clock CLOCK_MONOTONIC_RAW
#else
#define Use_Posix_Clock CLOCK_MONOTONIC
#endif  /* CLOCK_xxx */
#else   /* no suitable clock */
#error  No suitable monotonic clock!
#endif

#endif  /* time source */

#include "erl_nif.h"

/*********************************************************************/

#if     defined(Use_Mach_Time)
static  uint64_t    ms_divisor;

static int init_time()
{
    unsigned __int128           work;
    mach_timebase_info_data_t   info;
    (void) mach_timebase_info(& info);

    work  = (uint64_t)(info.denom);
    work *= 1000000uLL;
    work /= (uint64_t)(info.numer);
    ms_divisor = (uint64_t) work;
    return  0;
}

static ERL_NIF_TERM
monotonic_time_ms(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_uint64(env, (mach_absolute_time() / ms_divisor));
}

#elif   defined(Use_Posix_Clock)

static int init_time()
{
    struct timespec ts;
    /* we don't care about the resolution, only that the clock ID works */
    return  clock_getres(Use_Posix_Clock, & ts) ? errno : 0;
}

static ERL_NIF_TERM
monotonic_time_ms(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[])
{
    struct timespec ts;
    (void) clock_gettime(Use_Posix_Clock, & ts);
    return enif_make_uint64(env,
        (((uint64_t) ts.tv_sec) * 1000uL) + (((uint64_t) ts.tv_nsec) / 1000000uL));
}

#else   /* oops, shouldn't have gotten this far */
#error  No suitable monotonic clock!
#endif  /* time source */

/*********************************************************************/

static int on_load(ErlNifEnv * env,
    void ** priv_data, ERL_NIF_TERM load_info)
{
    return init_time();
}

static int on_upgrade(ErlNifEnv * env,
    void ** priv_data, void ** old_priv_data, ERL_NIF_TERM load_info)
{
    return init_time();
}

static void on_unload(ErlNifEnv * env, void * priv_data)
{
}

/*********************************************************************/

static ErlNifFunc nif_funcs[] = {
    {"monotonic_time_ms", 0, monotonic_time_ms}
};

ERL_NIF_INIT(riak_ensemble_clock, nif_funcs,
    on_load, NULL, on_upgrade, on_unload)
