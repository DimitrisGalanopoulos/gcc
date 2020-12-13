
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
int
gomp_stealing_policy_pass_work_quantity(struct gomp_group_work_share ** GWS, INT_T * work, int N, INT_T incr)
{
	struct gomp_group_work_share * gws;
	INT_T s, e, w, max = 0;
	int i, max_pos = 0;
	int n = 0;

	for (i=0;i<N;i++)
	{
		gws = GWS[i];
		s = __atomic_load_n(&gws->START, __ATOMIC_RELAXED);
		e = __atomic_load_n(&gws->END, __ATOMIC_RELAXED);
		w = gomp_group_work_share_iter_count(s, e, incr);
		work[i] = w;
		if (w > max)
		{
			max = w;
			max_pos = i;
		}
	}
	if (max == 0)
	{
		return 0;
	}

	if (gomp_hierarchical_stealing_sort_work_quantity)
	{
		struct gomp_group_work_share * GWS_buf[N];
		INT_T work_buf[N];
		INT_T work_quanta[N];      // Stores the simplified work amount.
		int exp = 7;               // We don't need to sort perfectly, so we sort in quanta of max.
		int m = 1 << exp;
		int offsets[m];
		int offset, total;

		for (i=0;i<m;i++)
			offsets[i] = 0;

		if (max >= m)
		{
			INT_T div;
			int div_exp;
			// Calculate max / m.
			div = max >> exp;
			// Find exponential of msb (div > 0).
			div_exp = msb_exp(div);
			// First power of 2 >= max / m.
			// Because of integer division error, we can't just compare with max / m.
			if (max >> div_exp >= m)
				div_exp++;
			// if (1 << div_exp < div)
				// div_exp++;
			// Calculate quanta.
			for (i=0;i<N;i++)
				work_quanta[i] = work[i] >> div_exp;
			// printf("%d: TEST 1: m=%d , max=%ld , div=%ld , div_exp=%d\n", omp_get_thread_num(), m, (long) max, (long) div, div_exp);
		}
		else
			for (i=0;i<N;i++)
				work_quanta[i] = work[i];


		for (i=0;i<N;i++)
			offsets[work_quanta[i]]++;

		total = 0;
		for (i=0;i<m;i++)
		{
			offset = offsets[i];
			offsets[i] = total;
			total += offset;
		}


		for (i=0;i<N;i++)
		{
			offset = offsets[work_quanta[i]];
			// printf("%d: TEST 4: quantum=%ld , offset=%d\n", omp_get_thread_num(), (long) work_quanta[i], offset);
			offsets[work_quanta[i]]++;
			work_buf[offset] = work[i];
			GWS_buf[offset] = GWS[i];
		}


		for (i=0;i<N;i++)
		{
			work[i] = work_buf[N-1-i];
			GWS[i] = GWS_buf[N-1-i];
		}
		n = N;
	}
	else
	{
		gws = GWS[0];              // Put max first.
		w = work[0];
		GWS[0] = GWS[max_pos];
		work[0] = work[max_pos];
		GWS[max_pos] = gws;
		work[max_pos] = w;
		n = 1;
		for (i=1;i<N;i++)
		{
			if (w > max/2)  // Keep those at least half the max.
			{
				GWS[n] = GWS[i];
				work[n] = work[i];
				n++;
			}
		}
	}
	return n;
}


static inline
int
gomp_stealing_policy_pass_cpu_node_locality(int my_tgnum, struct gomp_group_work_share ** GWS, int N)
{
	int my_cpu_node = (my_tgnum * gomp_max_thread_group_size) / gomp_cpu_node_size;
	struct gomp_group_work_share * gws;
	int owner_group, cpu_node;
	int n = 0;
	int i;
	for (i=0;i<N;i++)
	{
		gws = GWS[i];
		owner_group = __atomic_load_n(&gws->owner_group, __ATOMIC_RELAXED);
		cpu_node = (owner_group * gomp_max_thread_group_size) / gomp_cpu_node_size;
		if (cpu_node == my_cpu_node)
			GWS[n++] = gws;
	}
	if (n == 0)       // No local gws found, so all are valid.
		n = N;
	return n;
}


static inline
int
gomp_stealing_policy_pass_valid(int my_tgnum, struct gomp_group_work_share ** GWS)
{
	struct gomp_thread_group_data * tg;
	struct gomp_group_work_share * gws;
	int num_groups = gomp_get_num_thread_groups();
	int n = 0;
	int i, j;
	for (i=0;i<num_groups;i++)
	{
		if (i == my_tgnum)
			continue;
		tg = gomp_thread()->thread_pool->groups[i];
		j = __atomic_load_n(&tg->gws_current, __ATOMIC_RELAXED);
		if (j < 0)
			continue;
		gws = &tg->gws_buffer[j];
		if (__atomic_load_n(&gws->status, __ATOMIC_RELAXED) == GWS_READY)
			GWS[n++] = gws;
	}
	return n;
}


static inline
int
gomp_stealing_policy_passes(struct gomp_thread_group_data * group_data, struct gomp_group_work_share * data_holder, INT_T incr)
{
	int num_groups = group_data->num_groups;
	struct gomp_group_work_share * GWS[num_groups];
	INT_T work[num_groups];
	struct gomp_group_work_share * gws;
	INT_T s, e, w;
	int N = 0;
	int i;
	while (1)
	{
		N = gomp_stealing_policy_pass_valid(group_data->tgnum, GWS);
		if (N == 0)
			return 0;
		if (gomp_hierarchical_stealing_cpu_node_locality_pass)
			N = gomp_stealing_policy_pass_cpu_node_locality(group_data->tgnum, GWS, N);
		N = gomp_stealing_policy_pass_work_quantity(GWS, work, N, incr);
		if (N == 0)
			return 0;
		for (i=0;i<N;i++)
		{
			gws = GWS[i];
			if (__atomic_load_n(&gws->steal_lock, __ATOMIC_RELAXED))
				continue;
			if (__atomic_exchange_n(&gws->steal_lock, 1, __ATOMIC_SEQ_CST))
				continue;
			s = __atomic_load_n(&gws->START, __ATOMIC_RELAXED);
			e = __atomic_load_n(&gws->END, __ATOMIC_RELAXED);
			w = gomp_group_work_share_iter_count(s, e, incr);

			// Test if another thread stole before us.
			if (gomp_hierarchical_stealing_sort_work_quantity)
			{
				if ((i < N-1) && (w < work[i]/2) && (w < work[i+1]))      // No point reiterating when sorted, all gws are included.
				{
					__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
					continue;
				}
			}
			else 
			{
				if (w < work[0]/2)
				{
					__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
					continue;
				}
			}

			// Steal.
			if (incr < 0)
				w = -w;
			s = __atomic_fetch_add(&gws->START, w/2, __ATOMIC_RELAXED);
			e = s + w/2;
			if ((incr > 0 && e > gws->END) || (incr < 0 && e < gws->END))
				e = gws->END;
			data_holder->START = s;
			data_holder->END = e;
			data_holder->owner_group = gws->owner_group;
			__atomic_store_n(&gws->steal_lock, 0, __ATOMIC_SEQ_CST);
			return 1;
		}
	}
}


