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

static
struct gomp_group_work_share *
gomp_set_next_gws(struct gomp_thread_group_data * group_data, struct gomp_group_work_share * data_holder)
{
	PRINT_DEBUG("IN");
	struct gomp_group_work_share * gws, * gws_prev;
	int group_size = group_data->group_size;
	int n;

	n = group_data->gws_next_index;
	gws_prev = &group_data->gws_buffer[n];
	while (1)
	{
		// 'gws_buffer_size' must be > 2 for correctness.
		// No need for cpu_relax() here, we are sure to find a free 'gws' when 'gws_buffer_size' > 'group_size'.
		n = (n + 1) % group_data->gws_buffer_size;
		gws = &group_data->gws_buffer[n];
		if (!__atomic_load_n(&gws->workers_sem, __ATOMIC_RELAXED))
			break;
	}

	gws = &group_data->gws_buffer[n];

	__atomic_store_n(&group_data->gws_next_index, n, __ATOMIC_SEQ_CST);
	__atomic_store_n(&group_data->gws_next, gws, __ATOMIC_SEQ_CST);

	// Protect gws data from other groups (outer lock).
	while (1)
	{
		if (!__atomic_load_n(&gws->steal_lock, __ATOMIC_RELAXED))
			if (!__atomic_exchange_n(&gws->steal_lock, 1, __ATOMIC_SEQ_CST))
				break;
		cpu_relax();
	}

	// Protect gws data from group threads (inner lock).
	while (1)
	{
		// We are certainly not in this gws (workers_sem was 0 when first selected above).
		if (!__atomic_load_n(&gws->workers_sem, __ATOMIC_RELAXED))
		{
			int zero = 0;   // The atomic sets it every time, so we have to reinitialize it.
			if (__atomic_compare_exchange_n(&gws->workers_sem, &zero, -group_size, 0,  __ATOMIC_SEQ_CST,  __ATOMIC_RELAXED))
				break;
		}
		cpu_relax();
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

	// Release gws (unlock).
	__atomic_store_n(&gws->workers_sem, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&gws->status, GWS_READY, __ATOMIC_SEQ_CST);
	PRINT_DEBUG("OUT");
	return gws;
}


static
int
gomp_steal_gws(struct gomp_thread_data * t_data, struct gomp_thread_group_data * group_data, INT_T incr)
{
	struct gomp_group_work_share data_holder;
	PRINT_DEBUG("IN");

	if (__builtin_expect(!gomp_hierarchical_stealing, 0))
	{
		__atomic_store_n(&group_data->gws_next_index, -1, __ATOMIC_RELAXED);
		__atomic_store_n(&group_data->gws_next, NULL, __ATOMIC_RELAXED);
		PRINT_DEBUG("OUT");
		return -1;
	}

	do {
		if (!__atomic_load_n(&group_data->gws_next_lock, __ATOMIC_RELAXED))                               // Check that no one else is stealing.
			if (!__atomic_exchange_n(&group_data->gws_next_lock, 1, __ATOMIC_SEQ_CST))                // Claim lock.
			{
				if ((__atomic_load_n(&t_data->gws->status, __ATOMIC_RELAXED) == GWS_CLAIMED)      // Check that the gws is GWS_CLAIMED (finished) and that no one has stolen yet.
						&& (t_data->gws == group_data->gws_next))
					break;
				else
					__atomic_store_n(&group_data->gws_next_lock, 0, __ATOMIC_SEQ_CST);        // Release lock.
			}
		PRINT_DEBUG("OUT 0");
		return 0;
	} while (0);

	if (!gomp_stealing_policy_passes(group_data, &data_holder, incr))
	{
		__atomic_store_n(&group_data->gws_next_index, -1, __ATOMIC_RELAXED);
		__atomic_store_n(&group_data->gws_next, NULL, __ATOMIC_RELAXED);
		__atomic_store_n(&group_data->gws_next_lock, 0, __ATOMIC_SEQ_CST);                                // Release lock.
		PRINT_DEBUG("OUT -1");
		return -1;
	}

	gomp_set_next_gws(group_data, &data_holder);

	__atomic_store_n(&group_data->gws_next_lock, 0, __ATOMIC_SEQ_CST);                                        // Release lock.
	PRINT_DEBUG("OUT 1");
	return 1;
}


static inline
int
gomp_enter_gws(struct gomp_group_work_share * gws)
{
	PRINT_DEBUG("IN");
	if ((gws == NULL)
		|| (__atomic_load_n(&gws->status, __ATOMIC_RELAXED) == GWS_CLAIMED)
		|| (__atomic_fetch_add(&gws->workers_sem, 1, __ATOMIC_RELAXED) < 0))
	{
		PRINT_DEBUG("OUT -1");
		return -1;
	}
	PRINT_DEBUG("OUT 1");
	return 1;
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
gomp_get_next_gws(struct gomp_thread_data * t_data, struct gomp_thread_group_data * group_data, INT_T incr)
{
	PRINT_DEBUG("IN");
	struct gomp_group_work_share * gws_next;
	int n;
	if (t_data->gws != NULL)
		gomp_exit_gws(t_data->gws);
	while (1)
	{
		n = __atomic_load_n(&group_data->gws_next_index, __ATOMIC_RELAXED);
		if (n < 0)
		{
			t_data->gws = NULL;
			PRINT_DEBUG("OUT < 0");
			return NULL;
		}
		gws_next = &group_data->gws_buffer[n]; // We can't be sure that 'group_data->gws_next != NULL'.
		if (t_data->gws == gws_next)     // Check if no one has stolen yet.
		{
			int ret = gomp_steal_gws(t_data, group_data, incr);
			if (ret < 0)
			{
				t_data->gws = NULL;
				PRINT_DEBUG("OUT < 0");
				return NULL;
			}
			if (ret == 0)
			{
				while (__atomic_load_n(&group_data->gws_next_lock, __ATOMIC_RELAXED))      // Wait other thread to finish stealing.
					cpu_relax();
			}
			gws_next = __atomic_load_n(&group_data->gws_next, __ATOMIC_RELAXED);
			t_data->gws = gws_next;
		}
		if (gomp_enter_gws(gws_next) > 0)
		{
			t_data->gws = gws_next;
			PRINT_DEBUG("OUT");
			return gws_next;
		}
		cpu_relax();
	}
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
	__label__ NEXT_GWS;

	PRINT_DEBUG("IN");

	struct gomp_thread             * thr        = gomp_thread();
	struct gomp_thread_pool        * pool       = thr->thread_pool;
	struct gomp_thread_data        * t_data     = thr->t_data;
	struct gomp_thread_group_data  * group_data = t_data->group_data;
	struct gomp_group_work_share   * gws        = t_data->gws;

	INT_T chunk_start;
	INT_T chunk_end;
	INT_T grain_size = grain_size_init;      // 'grain_size' has the correct sign, matching 'incr'.

	if (__builtin_expect(gws == NULL, 0))    // First time in loop.
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
			// t_data->gws = gws;

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

			t_data->gws = NULL;
			gomp_get_next_gws(t_data, group_data, incr);
		}
		else
		{
			gomp_simple_barrier_wait(&pool->threads_dock);
			gomp_simple_barrier_wait(&pool->threads_dock);

			t_data->gws = NULL;
			gws = gomp_get_next_gws(t_data, group_data, incr);
			if (gws == NULL)
			{
				PRINT_DEBUG("OUT");
				return false;
			}
		}

		/*
		 * Degrade to static.
		 * Each thread should get a specific chunk, that depends only on the position
		 * of the thread in the group, the size of the group, and the boundaries of the
		 * group's iteration space (gws->start, gws->end).
		 */
		if (__builtin_expect(__atomic_load_n(&gomp_hierarchical_static, __ATOMIC_RELAXED) == 1, 0))
		{
			if (gws->START == gws->END)
			{
				PRINT_DEBUG("OUT");
				return false;
			}
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

	// If group_size == 1, then threshold should be zero,
	// else a race condition is formed, if all threads try to steal at the same time
	// while work still exists (threshold > 0) and therefor no work is being completed.
	INT_T steal_thres_coef = 2;
	INT_T steal_thres;
	steal_thres = steal_thres_coef * grain_size * (group_data->group_size - 1);
	while (1)
	{
		chunk_start = __atomic_fetch_add(&gws->START, grain_size, __ATOMIC_RELAXED);
		chunk_end = chunk_start + grain_size;

		if (incr > 0)
		{
			if (chunk_end + steal_thres > gws->END)
			{
				// if (gws == group_data->gws_next)   // Check that no one has stolen yet.
					// gomp_steal_gws(t_data, group_data, incr);
				if (chunk_end > gws->END)
				{
					if (chunk_start >= gws->END)
						goto NEXT_GWS;
					chunk_end = gws->END;
				}
			}
		}
		else
		{
			if (chunk_end + steal_thres < gws->END)
			{
				// if (gws == group_data->gws_next)   // Check that no one has stolen yet.
					// gomp_steal_gws(t_data, group_data, incr);
				if (chunk_end < gws->END)
				{
					if (chunk_start <= gws->END)
						goto NEXT_GWS;
					chunk_end = gws->END;
				}
			}
		}

		*pstart = chunk_start;
		*pend = chunk_end;

		PRINT_DEBUG("OUT");
		return true;

		NEXT_GWS:
			// We have to be certain the gws is claimed before used in the future.
			// Also so that we don't aimlessly reenter and interfere with the previous work after it is depleted.
			if (__atomic_load_n(&gws->status, __ATOMIC_RELAXED) != GWS_CLAIMED)
				__atomic_store_n(&gws->status, GWS_CLAIMED, __ATOMIC_SEQ_CST);
			gws = gomp_get_next_gws(t_data, group_data, incr);
			if (t_data->gws == NULL)
			{
				PRINT_DEBUG("OUT");
				return false;
			}
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

