#include "hier_sched_structs.h"


#ifdef HIER_ULL
	#define INT_T long long
	#define START start_ull
	#define END end_ull
	#define GRAIN_SIZE grain_size_ull
	#define ITER iter_ull
	#define	WORK work_ull
#else
	#define INT_T long
	#define START start
	#define END end
	#define GRAIN_SIZE grain_size
	#define ITER iter
	#define	WORK work
#endif


static inline
void
cpu_relax()
{
	// __asm("pause");
	// __asm volatile ("" : : : "memory");
	// __asm volatile ("pause" : : : "memory");
	__asm volatile ("rep; pause" : : : "memory");
	// __asm volatile ("rep; nop" : : : "memory");

	// for (int i=0;i<count;i++)
		// __asm volatile ("rep; pause" : : : "memory");

}


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--------------------------------------------------------------------------------------------------------------------------------------------
-                                                          Work Share Functions                                                            -
--------------------------------------------------------------------------------------------------------------------------------------------
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/


/*
 * bool __atomic_compare_exchange_n (type *ptr, type *expected, type desired, bool weak, int success_memorder, int failure_memorder)
 *     This built-in function implements an atomic compare and exchange operation.
 *     This compares the contents of *ptr with the contents of *expected.
 *     If equal, the operation is a read-modify-write operation that writes desired into *ptr.
 *     If they are not equal, the operation is a read and the current contents of *ptr are written into *expected.
 *     weak is true for weak compare_exchange, which may fail spuriously, and false for the strong variation, which never fails spuriously.
 *     Many targets only offer the strong variation and ignore the parameter. When in doubt, use the strong variation.
 *
 *     If desired is written into *ptr then true is returned and memory is affected according to the memory order specified by success_memorder.
 *     There are no restrictions on what memory order can be used here.
 *
 *     Otherwise, false is returned and memory is affected according to failure_memorder.
 *     This memory order cannot be __ATOMIC_RELEASE nor __ATOMIC_ACQ_REL.
 *     It also cannot be a stronger order than that specified by success_memorder.
 */

// All 'gomp_group_work_share' structs of the group should be in 'GWS_CLAIMED' state
// when we enter this function.
static
struct gomp_group_work_share *
gomp_set_next_gws(struct gomp_thread_group_data * group_data, struct gomp_group_work_share * data_holder)
{
	PRINT_DEBUG("IN");
	struct gomp_group_work_share * gws, * gws_prev;
	int group_size = group_data->group_size;
	int n;

	n = group_data->gws_current;
	gws_prev = &group_data->gws_buffer[n];
	do {
		n = (n + 1) % group_data->gws_buffer_size;
		gws = &group_data->gws_buffer[n];
	} while (__atomic_load_n(&gws->workers_sem, __ATOMIC_RELAXED));

	// n = (group_data->gws_current + 1) % group_data->gws_buffer_size;

	gws = &group_data->gws_buffer[n];

	// Better to move the waiting crowd to the new work early,
	// to not uselessly interfere with the previous work, but after claiming it.
	__atomic_store_n(&group_data->gws_current, n, __ATOMIC_SEQ_CST);

	while (1)
	{
		if (!__atomic_load_n(&gws->steal_lock, __ATOMIC_RELAXED))
			if (!__atomic_exchange_n(&gws->steal_lock, 1, __ATOMIC_SEQ_CST))
				break;
	}
	while (1)
	{
		if (!__atomic_load_n(&gws->workers_sem, __ATOMIC_RELAXED))
		{
			int zero = 0;   // The atomic sets it every time, so we have to reinitialize it.
			if (__atomic_compare_exchange_n(&gws->workers_sem, &zero, -group_size, 0,  __ATOMIC_SEQ_CST,  __ATOMIC_RELAXED))
				break;
		}
	}

	__atomic_store_n(&gws->START, data_holder->START, __ATOMIC_RELAXED);
	__atomic_store_n(&gws->END, data_holder->END, __ATOMIC_RELAXED);

	__atomic_store_n(&gws->owner_group, data_holder->owner_group, __ATOMIC_RELAXED);


	if (__atomic_load_n(&gomp_use_after_stealing_group_fun, __ATOMIC_RELAXED))
	{
		while (__atomic_load_n(&(gws_prev->workers_sem), __ATOMIC_RELAXED) > 0)       // Wait for slaves to exit the previous work share.
			cpu_relax();
		gomp_after_stealing_group_fun(data_holder->owner_group, data_holder->START, data_holder->END);
	}

	__atomic_store_n(&gws->workers_sem, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->status, GWS_READY, __ATOMIC_SEQ_CST);
	PRINT_DEBUG("OUT");
	return gws;
}


static inline
bool
gomp_enter_gws(struct gomp_group_work_share * gws)
{
	if (__atomic_load_n(&gws->status, __ATOMIC_RELAXED) == GWS_CLAIMED)
		return false;
	if (__atomic_fetch_add(&gws->workers_sem, 1, __ATOMIC_RELAXED) < 0)
		return false;
	return true;
}


static inline
void
gomp_exit_gws(struct gomp_group_work_share * gws)
{
	PRINT_DEBUG("IN");
	__atomic_fetch_add(&gws->workers_sem, -1, __ATOMIC_RELAXED);
	PRINT_DEBUG("OUT");
}


static
struct gomp_group_work_share *
gomp_get_next_gws(struct gomp_thread_group_data * group_data, struct gomp_group_work_share * gws_prev)
{
	PRINT_DEBUG("IN");
	struct gomp_group_work_share * gws_next;
	int n;
	if (gws_prev != NULL)
		gomp_exit_gws(gws_prev);
	while (1)
	{
		n = __atomic_load_n(&group_data->gws_current, __ATOMIC_RELAXED);
		if (n < 0)
		{
			PRINT_DEBUG("OUT");
			return NULL;
		}
		gws_next = &group_data->gws_buffer[n];
		if (gomp_enter_gws(gws_next))
		{
			PRINT_DEBUG("OUT");
			return gws_next;
		}
		// cpu_relax();
	}
}


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--------------------------------------------------------------------------------------------------------------------------------------------
-                                                            Stealing Policies                                                             -
--------------------------------------------------------------------------------------------------------------------------------------------
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/


static inline
INT_T
gomp_group_work_share_iter_count(INT_T start, INT_T end, INT_T incr)
{
	INT_T work = (incr > 0) ? end - start : start - end;
	if (work < 0)      // No work left.
		work = 0;
	return work;
}


// #include "stealing_policy.h"
#include "stealing_policy_scores.h"


static
struct gomp_group_work_share *
gomp_steal_gws(struct gomp_thread_group_data * group_data, INT_T incr)
{
	PRINT_DEBUG("IN");
	struct gomp_group_work_share * gws;
	struct gomp_group_work_share data_holder;

	if (__builtin_expect(!gomp_hierarchical_stealing, 0))
	{
		PRINT_DEBUG("OUT");
		return NULL;
	}

	if (!gomp_stealing_policy_passes(group_data, &data_holder, incr))
	{
		PRINT_DEBUG("OUT");
		return NULL;
	}

	gws = gomp_set_next_gws(group_data, &data_holder);
	PRINT_DEBUG("OUT");
	return gws;
}


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--------------------------------------------------------------------------------------------------------------------------------------------
-                                                         Scheduling Hierarchical                                                          -
--------------------------------------------------------------------------------------------------------------------------------------------
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/


/*
 * The '!=' cannot be used as a test operator, as it makes it impossible
 * in many situations to conclude about the number of iterations.
 */

static inline
void
gomp_initialize_group_work(int num_workers, int worker_pos, INT_T start, INT_T end, INT_T * local_start, INT_T * local_end)
{
	INT_T len         = end - start;
	INT_T per_t_len   = len / num_workers;
	INT_T rem         = len - per_t_len * num_workers;
	if (rem != 0)
	{
		INT_T s = (rem > 0) ? 1 : -1;
		if (worker_pos < s * rem)
		{
			per_t_len += s;
			rem = 0;
		}
	}
	*local_start = start + per_t_len * worker_pos + rem;
	*local_end   = *local_start + per_t_len;
}


static inline
bool
gomp_iter_l_ull_hierarchical_next(INT_T grain_size_init, INT_T start, INT_T end, INT_T incr, INT_T * pstart, INT_T * pend)
{
	__label__ MASTER_NEXT_GWS, SLAVE_NEXT_GWS;

	PRINT_DEBUG("IN");

	struct gomp_thread             * thr        = gomp_thread();
	struct gomp_thread_pool        * pool       = thr->thread_pool;
	struct gomp_thread_data        * t_data     = thr->t_data;
	struct gomp_thread_group_data  * group_data = t_data->group_data;
	struct gomp_group_work_share   * gws        = t_data->gws;
	struct gomp_group_work_share   * gws_next   = t_data->gws_next;

	INT_T chunk_start;
	INT_T chunk_end;
	INT_T grain_size = grain_size_init;      // 'grain_size' is given with the correct sign.

	if (__builtin_expect(gws == NULL, 0))
	{
		gomp_simple_barrier_wait(&pool->threads_dock);

		if (t_data->tgpos == 0)     // group master
		{
			struct gomp_group_work_share data_holder;
			// User loop partitioner.
			if (__atomic_load_n(&gomp_use_custom_loop_partitioner, __ATOMIC_RELAXED))
			{
				long tmp_start, tmp_end;
				gomp_loop_partitioner(start, end, &tmp_start, &tmp_end);
				data_holder.START = tmp_start;
				data_holder.END = tmp_end;
			}
			else
				gomp_initialize_group_work(group_data->num_groups, t_data->tgnum, start, end, &data_holder.START, &data_holder.END);
			data_holder.owner_group = t_data->tgnum;
			gws = gomp_set_next_gws(group_data, &data_holder);
			gws_next = gws;
			t_data->gws = gws;
			t_data->gws_next = gws;

			gomp_simple_barrier_wait(&pool->threads_dock);

			// After-stealing user functions.
			// Initialized after the first 'gomp_set_next_gws()', which would call the group function.
			if (thr->ts.team_id == 0)   // team master
			{
				if (__atomic_load_n(&gomp_use_after_stealing_group_fun_next_loop, __ATOMIC_RELAXED))
				{
					__atomic_store_n(&gomp_use_after_stealing_group_fun, 1, __ATOMIC_RELAXED);
					__atomic_store_n(&gomp_use_after_stealing_group_fun_next_loop, 0, __ATOMIC_RELAXED);
					__atomic_store_n(&gomp_after_stealing_group_fun, gomp_after_stealing_group_fun_next_loop, __ATOMIC_RELAXED);
				}
				else
					__atomic_store_n(&gomp_use_after_stealing_group_fun, 0, __ATOMIC_RELAXED);

				if (__atomic_load_n(&gomp_use_after_stealing_thread_fun_next_loop, __ATOMIC_RELAXED))
				{
					__atomic_store_n(&gomp_use_after_stealing_thread_fun, 1, __ATOMIC_RELAXED);
					__atomic_store_n(&gomp_use_after_stealing_thread_fun_next_loop, 0, __ATOMIC_RELAXED);
					__atomic_store_n(&gomp_after_stealing_thread_fun, gomp_after_stealing_thread_fun_next_loop, __ATOMIC_RELAXED);
				}
				else
					__atomic_store_n(&gomp_use_after_stealing_thread_fun, 0, __ATOMIC_RELAXED);
			}

			// User loop partitioner.
			if (thr->ts.team_id == 0)   // team master
			{
				__atomic_store_n(&gomp_use_custom_loop_partitioner, 0, __ATOMIC_RELAXED);
			}

			gomp_simple_barrier_wait(&pool->threads_dock);
		}
		else
		{
			gomp_simple_barrier_wait(&pool->threads_dock);
			gomp_simple_barrier_wait(&pool->threads_dock);

			gws = gomp_get_next_gws(group_data, NULL);
			gws_next = gws;
			if (gws == NULL)
			{
				PRINT_DEBUG("OUT");
				return false;
			}
			t_data->gws = gws;
			t_data->gws_next = gws;
		}

		/*
		 * Degrade to static.
		 * Each thread should get a specific chunk, that depends only on the position
		 * of the thread in the group, the size of the group, and the boundaries of the
		 * group's iteration space (gws->start, gws->end).
		 */
		if (__builtin_expect(__atomic_load_n(&gomp_hierarchical_static, __ATOMIC_RELAXED) == 1, 0))
		{
			gomp_initialize_group_work(group_data->group_size, t_data->tgpos, gws->START, gws->END, &chunk_start, &chunk_end);

			gomp_simple_barrier_wait(&pool->threads_dock);

			if (t_data->tgpos == 0)     // group master
				gws->START = gws->END;

			gomp_simple_barrier_wait(&pool->threads_dock);

			*pstart = chunk_start;
			*pend = chunk_end;
			gomp_simple_barrier_wait(&pool->threads_dock);
			PRINT_DEBUG("OUT");
			return true;
		}
	}

	while (1)
	{
		chunk_start = __atomic_fetch_add(&gws->START, grain_size, __ATOMIC_RELAXED);
		chunk_end = chunk_start + grain_size;

		if (__builtin_expect(t_data->tgpos == 0, 0))
		{
			// If group_size == 1, then threshold should be zero,
			// else a race condition is formed, if all threads try to steal at the same time
			// while work still exists (threshold > 0) and therefor no work is being completed.
			INT_T steal_thres_coef = 2;
			INT_T steal_thres;
			steal_thres = steal_thres_coef * grain_size * (group_data->group_size - 1);
			if (incr > 0)
			{
				if (chunk_end + steal_thres > gws->END)
				{
					if (gws_next == gws)
					{
						gws_next = gomp_steal_gws(group_data, incr);
						t_data->gws_next = gws_next;
					}
					if (chunk_end > gws->END)
					{
						if (chunk_start >= gws->END)
							goto MASTER_NEXT_GWS;
						chunk_end = gws->END;
					}
				}
			}
			else
			{
				if (chunk_end + steal_thres < gws->END)
				{
					if (gws_next == gws)
					{
						gws_next = gomp_steal_gws(group_data, incr);
						t_data->gws_next = gws_next;
					}
					if (chunk_end < gws->END)
					{
						if (chunk_start <= gws->END)
							goto MASTER_NEXT_GWS;
						chunk_end = gws->END;
					}
				}
			}
		}
		else
		{
			if (incr > 0)
			{
				if (chunk_end > gws->END)
				{
					if (chunk_start >= gws->END)
						goto SLAVE_NEXT_GWS;
					chunk_end = gws->END;
				}
			}
			else
			{
				if (chunk_end < gws->END)
				{
					if (chunk_start <= gws->END)
						goto SLAVE_NEXT_GWS;
					chunk_end = gws->END;
				}
			}
		}

		*pstart = chunk_start;
		*pend = chunk_end;

		PRINT_DEBUG("OUT");
		return true;

		MASTER_NEXT_GWS:
			// We have to be certain the gws is claimed before used in the future.
			__atomic_store_n(&gws->status, GWS_CLAIMED, __ATOMIC_SEQ_CST);
			if (gws_next == NULL)
			{
				__atomic_store_n(&group_data->gws_current, -1, __ATOMIC_RELAXED);
				t_data->gws = NULL;
				t_data->gws_next = NULL;
				PRINT_DEBUG("OUT");
				return false;
			}
			gws = gws_next;
			t_data->gws = gws_next;
			t_data->gws_next = gws_next;
			if (__atomic_load_n(&gomp_use_after_stealing_thread_fun, __ATOMIC_RELAXED))
				gomp_after_stealing_thread_fun(gws->owner_group, gws->START, gws->END);
			continue;

		SLAVE_NEXT_GWS:
			// Slaves also claim gws, so that they don't aimlessly reenter it after it is depleted.
			if (gws->status != GWS_CLAIMED)
				__atomic_store_n(&gws->status, GWS_CLAIMED, __ATOMIC_SEQ_CST);
			gws = gomp_get_next_gws(group_data, gws);
			if (gws == NULL)
			{
				t_data->gws = NULL;
				t_data->gws_next = NULL;
				PRINT_DEBUG("OUT");
				return false;
			}
			t_data->gws = gws;
			if (__atomic_load_n(&gomp_use_after_stealing_thread_fun, __ATOMIC_RELAXED))
				gomp_after_stealing_thread_fun(gws->owner_group, gws->START, gws->END);
	}
}


#undef INT_T
#undef START
#undef END
#undef GRAIN_SIZE
#undef ITER
#undef WORK

