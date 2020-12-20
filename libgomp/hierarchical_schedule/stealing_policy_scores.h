
// Returns the power of 2 exponential of the msb of input.
static inline
int
msb_exp(INT_T x)
{
	int bits = sizeof(x)*8;
	int i;
	if (x == 0)
		return -1;
	for (i=0;i<bits;i++)
	{
		x >>= 1;
		if (x == 0)
			break;
	}
	return i;
}


static inline
void
gomp_stealing_policy_pass_sort(struct gomp_stealing_passes_data ** SPD, int N)
{
	PRINT_DEBUG("IN");
	struct gomp_stealing_passes_data * SPD_buf[N];
	int score_ceil = 0;
	int i, score, total, offset;

	for (i=0;i<N;i++)
	{
		score = SPD[i]->score;
		if (score > score_ceil)
			score_ceil = score;
	}
	score_ceil++;

	int offsets[score_ceil];

	for (i=0;i<score_ceil;i++)
		offsets[i] = 0;

	for (i=0;i<N;i++)
		offsets[SPD[i]->score]++;

	total = 0;
	for (i=0;i<score_ceil;i++)
	{
		offset = offsets[i];
		offsets[i] = total;
		total += offset;
	}

	for (i=0;i<N;i++)
	{
		score = SPD[i]->score;
		offset = offsets[score];
		SPD_buf[offset] = SPD[i];
		offsets[score]++;
	}

	for (i=0;i<N;i++)
		SPD[i] = SPD_buf[N-1-i];
	PRINT_DEBUG("OUT");
}


static inline
void
gomp_stealing_policy_pass_cpu_node_locality(struct gomp_stealing_passes_data ** SPD, int N, int my_tgnum)
{
	PRINT_DEBUG("IN");
	int my_cpu_node = (my_tgnum * gomp_max_thread_group_size) / gomp_cpu_node_size;
	struct gomp_stealing_passes_data * spd;
	struct gomp_group_work_share * gws;
	int owner_group, cpu_node;
	int i;
	for (i=0;i<N;i++)
	{
		spd = SPD[i];
		gws = spd->gws;
		owner_group = __atomic_load_n(&gws->owner_group, __ATOMIC_RELAXED);
		cpu_node = (owner_group * gomp_max_thread_group_size) / gomp_cpu_node_size;
		if (cpu_node == my_cpu_node)
		{
			spd->score += 1;
		}
	}
	PRINT_DEBUG("OUT");
}


static inline
void
gomp_stealing_policy_pass_quantize_work(struct gomp_stealing_passes_data ** SPD, int N, INT_T max_work)
{
	PRINT_DEBUG("IN");
	int quanta_exp = 6;                    // We don't need to sort perfectly, so we sort in quanta of max work.
	int quanta_ceil = 1 << quanta_exp;
	int i;
	if (max_work >= quanta_ceil)
	{
		INT_T div;
		int div_exp;
		// Calculate max_work / quanta_ceil.
		div = max_work >> quanta_exp;
		// Find exponential of msb (div > 0).
		div_exp = msb_exp(div);
		// First power of 2 >= max_work / quanta_ceil.
		// Because of integer division error, we can't just compare with max_work / quanta_ceil.
		if (max_work >> div_exp >= quanta_ceil)
			div_exp++;
		// Calculate quanta.
		for (i=0;i<N;i++)
			SPD[i]->score = SPD[i]->WORK >> div_exp;
		// printf("%d: TEST 1: quanta_ceil=%d , max_work=%ld , div=%ld , div_exp=%d\n", omp_get_thread_num(), quanta_ceil, (long) max_work, (long) div, div_exp);
	}
	else
		for (i=0;i<N;i++)
			SPD[i]->score = SPD[i]->WORK;
	PRINT_DEBUG("OUT");
}


// Calculates work and puts the gws with the max work first.
static inline
int
gomp_stealing_policy_pass_max_work(struct gomp_stealing_passes_data ** SPD, int N, INT_T incr)
{
	PRINT_DEBUG("IN");
	struct gomp_stealing_passes_data * spd;
	struct gomp_group_work_share * gws;
	int n = 0;
	INT_T s, e, w, max_work = 0;
	int i, max_pos = 0;
	for (i=0;i<N;i++)
	{
		spd = SPD[i];
		gws = spd->gws;
		s = __atomic_load_n(&gws->START, __ATOMIC_RELAXED);
		e = __atomic_load_n(&gws->END, __ATOMIC_RELAXED);
		w = gomp_group_work_share_iter_count(s, e, incr);
		if (w > 0)
		{
			spd->WORK = w;
			SPD[n] = spd;
			if (w > max_work)
			{
				max_work = w;
				max_pos = n;
			}
			n++;
		}
	}
	spd = SPD[0];
	SPD[0] = SPD[max_pos];
	SPD[max_pos] = spd;
	PRINT_DEBUG("OUT");
	return n;
}


static inline
int
gomp_stealing_policy_pass_valid(int my_tgnum, struct gomp_stealing_passes_data * SPD_structs, struct gomp_stealing_passes_data ** SPD)
{
	PRINT_DEBUG("IN");
	int num_groups = gomp_get_num_thread_groups();
	struct gomp_thread_group_data * group_data;
	struct gomp_group_work_share * gws;
	int n = 0;
	int i, j;
	for (i=0;i<num_groups;i++)
	{
		if (i == my_tgnum)
			continue;
		group_data = gomp_thread()->thread_pool->groups[i];
		j = __atomic_load_n(&group_data->gws_next_index, __ATOMIC_RELAXED);
		if (j < 0)
			continue;
		gws = &group_data->gws_buffer[j];
		if (__atomic_load_n(&gws->status, __ATOMIC_RELAXED) == GWS_READY)
		{
			SPD_structs[n].gws = gws;
			SPD[n] = &SPD_structs[n];
			n++;
		}
	}
	PRINT_DEBUG("OUT");
	return n;
}


static inline
int
gomp_stealing_policy_passes(struct gomp_thread_group_data * group_data, struct gomp_group_work_share * data_holder, INT_T incr)
{
	PRINT_DEBUG("IN");
	int num_groups = group_data->num_groups;
	struct gomp_stealing_passes_data SPD_structs[num_groups];
	struct gomp_stealing_passes_data * SPD[num_groups];

	struct gomp_group_work_share * gws;
	INT_T s, e, w, w_steal;
	int N = 0;
	int i;

	while (1)
	{
		N = gomp_stealing_policy_pass_valid(group_data->tgnum, SPD_structs, SPD);
		if (N == 0)
		{
			PRINT_DEBUG("OUT");
			return 0;
		}
		N = gomp_stealing_policy_pass_max_work(SPD, N, incr);
		if (N == 0)
		{
			PRINT_DEBUG("OUT");
			return 0;
		}
		if (gomp_hierarchical_stealing_scores)
		{
			gomp_stealing_policy_pass_quantize_work(SPD, N, SPD[0]->WORK);
			if (gomp_hierarchical_stealing_cpu_node_locality_pass)
				gomp_stealing_policy_pass_cpu_node_locality(SPD, N, group_data->tgnum);
			gomp_stealing_policy_pass_sort(SPD, N);
		}

		for (i=0;i<N;i++)
		{
			gws = SPD[i]->gws;
			if (__atomic_load_n(&gws->steal_lock, __ATOMIC_RELAXED))
				continue;
			if (__atomic_exchange_n(&gws->steal_lock, 1, __ATOMIC_SEQ_CST))
				continue;
			s = __atomic_load_n(&gws->START, __ATOMIC_RELAXED);
			e = __atomic_load_n(&gws->END, __ATOMIC_RELAXED);
			w = gomp_group_work_share_iter_count(s, e, incr);
			w_steal = w / 2;          // Always leaves at least 1 iteration to the victim group, so progress is guaranteed.
			if (w_steal <= 0)
			{
				__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
				continue;
			}

			// Test if another thread stole before us.
			// If true, compare with the amount of work of the next gws in queue.
			if (gomp_hierarchical_stealing_scores)
			{
				if ((w < SPD[i]->work / 2) && ((i >= N-1) || (w < SPD[i+1]->work)))
				{
					__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
					continue;
				}
			}
			else
			{
				if (w < SPD[0]->work / 2)   // If we don't sort, keep only those at least half the max.
				{
					__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
					continue;
				}
			}

			// Steal.
			if (incr < 0)
				w_steal = -w_steal;
			s = __atomic_fetch_add(&gws->START, w_steal, __ATOMIC_RELAXED);
			e = s + w_steal;
			if ((incr > 0 && e > gws->END) || (incr < 0 && e < gws->END))
				e = gws->END;
			data_holder->START = s;
			data_holder->END = e;
			data_holder->owner_group = gws->owner_group;
			__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
			PRINT_DEBUG("OUT steal_from_group=%d, s=%ld, e=%ld", gws->owner_group, (long) s, (long) e);
			return 1;
		}
	}
}


