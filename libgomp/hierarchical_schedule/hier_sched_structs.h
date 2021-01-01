#ifndef TPOOL_STRUCTS_H
#define TPOOL_STRUCTS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <float.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/syscall.h>       // syscall()
#include <pthread.h>

#include "macros.h"


//==========================================================================================================================================
//= Barrier
//==========================================================================================================================================


struct gomp_barrier_data {
	int num_threads;
	int flag;
	int counter __attribute__ ((aligned (CACHE_LINE_SIZE)));
	char padding[0] __attribute__ ((aligned (CACHE_LINE_SIZE)));
} __attribute__ ((aligned (CACHE_LINE_SIZE)));


static inline
void
gomp_barrier_data_init(struct gomp_barrier_data * barrier, int num_threads)
{
	__atomic_store_n(&barrier->num_threads, num_threads, __ATOMIC_RELAXED);
	__atomic_store_n(&barrier->flag, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&barrier->counter, 0, __ATOMIC_RELAXED);
}


//==========================================================================================================================================
//= Stealing Passes Data
//==========================================================================================================================================


struct gomp_stealing_passes_data {
	struct gomp_group_work_share * gws;
	union {
		long work;
		long long work_ull;
	};
	int score;
};


// static
// void
// gomp_snprintf_spd(char * buf, int n, struct gomp_stealing_passes_data * spd)
// {
	// snprintf(buf, n, "group=%d , work=%ld , score=%d\n", spd->gws->owner_group, spd->work, spd->score);
// }


//==========================================================================================================================================
//= Work Share
//==========================================================================================================================================


typedef enum {
	GWS_CLAIMED,
	GWS_READY,
} gomp_group_work_share_status_t;


struct gomp_group_work_share {
	gomp_group_work_share_status_t status;
	int owner_group;

	union {
		long end;
		long long end_ull;
	};

	union {
		long start;
		long long start_ull;
	} __attribute__ ((aligned (CACHE_LINE_SIZE)));

	int workers_sem __attribute__ ((aligned (CACHE_LINE_SIZE)));

	int steal_lock __attribute__ ((aligned (CACHE_LINE_SIZE)));

	char padding[0] __attribute__ ((aligned (CACHE_LINE_SIZE)));
} __attribute__ ((aligned (CACHE_LINE_SIZE)));


static inline
void
gomp_group_work_share_init(struct gomp_group_work_share * gws, int max_group_size)
{
	gws->end = 0;
	gws->start = 0;

	gws->owner_group = 0;

	__atomic_store_n(&gws->status, GWS_CLAIMED, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->workers_sem, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
}


//==========================================================================================================================================
//= Thread Group Data
//==========================================================================================================================================


// Data holding information about the thread group.
struct gomp_thread_group_data {
	int tgnum;
	int max_group_size;    // max thread group size
	int group_size;        // actual size of this group (usually == max_group_size)
	int num_groups;
	int master_tnum;

	struct gomp_group_work_share * gws_buffer __attribute__ ((aligned (CACHE_LINE_SIZE)));
	int gws_buffer_size;   // > 1 to work correctly.
	int gws_next_index;
	struct gomp_group_work_share * gws_next;
	int gws_next_lock;

	struct gomp_barrier_data * inner_barrier;

	char padding[0] __attribute__ ((aligned (CACHE_LINE_SIZE)));
} __attribute__ ((aligned (CACHE_LINE_SIZE)));


static inline
void
gomp_thread_group_data_init(struct gomp_thread_group_data * tg_data, int tgnum, int max_group_size, int group_size, int num_groups, int master_tnum)
{
	int i;

	tg_data->tgnum          = tgnum;
	tg_data->max_group_size = max_group_size;
	tg_data->group_size     = group_size;
	tg_data->num_groups     = num_groups;
	tg_data->master_tnum    = master_tnum;

	tg_data->gws_buffer_size = 8*group_size + 2;    // > 1 to work correctly.
	tg_data->gws_next_index  = -1;
	tg_data->gws_buffer      = gomp_malloc(tg_data->gws_buffer_size * sizeof(*tg_data->gws_buffer));
	__atomic_store_n(&tg_data->gws_next, NULL, __ATOMIC_RELAXED);
	__atomic_store_n(&tg_data->gws_next_lock, 0, __ATOMIC_SEQ_CST);
	tg_data->inner_barrier   = gomp_malloc(sizeof(*tg_data->inner_barrier));
	gomp_barrier_data_init(tg_data->inner_barrier, group_size);
	for (i=0;i<tg_data->gws_buffer_size;i++)
		gomp_group_work_share_init(&tg_data->gws_buffer[i], max_group_size);
}


//==========================================================================================================================================
//= Thread Data
//==========================================================================================================================================


// Data holding information about the threads.
struct gomp_thread_data {
	pthread_t tid;
	int tnum;
	int tgnum;
	int tgpos;                 // position in group
	int max_group_size;        // max thread group size
	int num_threads;
	struct gomp_thread_group_data * group_data;
	struct gomp_group_work_share * gws;

	char padding[0] __attribute__ ((aligned (CACHE_LINE_SIZE)));
} __attribute__ ((aligned (CACHE_LINE_SIZE)));


static inline
void
gomp_thread_data_init(struct gomp_thread_data * t_data, struct gomp_thread_group_data ** groups, int tnum, int max_threads, int max_group_size)
{
	int group_size, num_groups;

	t_data->tid            = pthread_self();
	t_data->tnum           = tnum;
	t_data->tgnum          = tnum / max_group_size;
	t_data->tgpos          = tnum % max_group_size;
	t_data->max_group_size = max_group_size;
	t_data->num_threads    = max_threads;
	t_data->gws            = NULL;
	if (t_data->tgpos == 0)    // group master
	{
		t_data->group_data = gomp_malloc(sizeof(*t_data->group_data));

		group_size = MIN(max_group_size, max_threads - tnum);
		__atomic_store_n(&groups[t_data->tgnum], t_data->group_data, __ATOMIC_RELAXED);
		num_groups = (max_threads + max_group_size - 1) / max_group_size;
		gomp_thread_group_data_init(t_data->group_data, t_data->tgnum, max_group_size, group_size, num_groups, tnum);
		// printf("tm-- %d %d %d %d\n", t_data->tnum, t_data->tgnum, t_data->tgpos, t_data->group_data->master_tnum);
	}
	else
	{
		t_data->group_data = NULL;
		// printf("t--- %d %d %d %d\n", t_data->tnum, t_data->tgnum, t_data->tgpos, t_data->tnum - t_data->tgpos);
	}
}


static inline
void
gomp_set_thread_group_data(struct gomp_thread_data * t_data, struct gomp_thread_group_data ** groups)
{
	if (t_data->tgpos != 0)    // group slave
	{
		t_data->group_data = __atomic_load_n(&groups[t_data->tgnum], __ATOMIC_RELAXED);
		// printf("g--- %d %d %d %d\n", t_data->tnum, t_data->tgnum, t_data->tgpos, t_data->group_data->master_tnum);
	}
	else
	{
		// printf("gm-- %d %d %d %d\n", t_data->tnum, t_data->tgnum, t_data->tgpos, t_data->group_data->master_tnum);
	}
}


//==========================================================================================================================================
//= Utilities
//==========================================================================================================================================


static inline struct gomp_thread_data * gomp_get_thread_data() { return gomp_thread()->t_data; }


// static inline int gomp_get_max_threads()   { return gomp_thread()->thread_pool->threads_used; }


static inline int gomp_get_max_thread_group_size() { return gomp_max_thread_group_size; }
static inline int gomp_get_thread_group_size()     { return gomp_thread()->t_data->group_data->group_size; }
static inline int gomp_get_num_thread_groups()     { return gomp_num_thread_groups; }


#endif /* TPOOL_STRUCTS_H */


