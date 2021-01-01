#ifndef BARRIER_H
#define BARRIER_H


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


// Spins while *flag == val.
static inline
void
do_spin(int * flag, int val)
{
	while (1)
	{
		if (__builtin_expect(__atomic_load_n(flag, __ATOMIC_RELAXED) != val, 0))
			return;
		cpu_relax();
	}
}


static inline
int
barrier_wait(struct gomp_barrier_data * barrier)
{
	int num_threads = barrier->num_threads;
	int * flag      = &barrier->flag;
	int * counter   = &barrier->counter;
	int spin_val    = __atomic_load_n(flag, __ATOMIC_SEQ_CST);

	int n = __atomic_add_fetch(counter, 1, __ATOMIC_SEQ_CST);
	if (__builtin_expect(n == num_threads, 0))
		return 1;
	do_spin(flag, spin_val);
	return 0;
}


static inline
void
barrier_release(struct gomp_barrier_data * barrier)
{
	int * flag    = &barrier->flag;
	int * counter = &barrier->counter;
	__atomic_store_n(counter, 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(flag, !(*flag), __ATOMIC_SEQ_CST);
}


static inline
void
group_inner_barrier_wait(struct gomp_thread_group_data * group_data)
{
	if (barrier_wait(group_data->inner_barrier))
		barrier_release(group_data->inner_barrier);
}


static inline
void
group_outer_barrier_wait(struct gomp_thread_group_data * group_data)
{
	if (barrier_wait(group_data->inner_barrier))
	{
		if (barrier_wait(&gomp_group_outer_barrier))
			barrier_release(&gomp_group_outer_barrier);
		barrier_release(group_data->inner_barrier);
	}
}



#endif /* BARRIER_H */

