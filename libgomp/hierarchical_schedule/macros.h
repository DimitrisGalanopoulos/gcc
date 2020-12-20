#ifndef MACROS_H
#define MACROS_H

#include <stdio.h>
#include <stdlib.h>


#define PRINT_DEBUG(format, ...)

/* #define PRINT_DEBUG(format, ...)                                                                                                            \
do {                                                                                                                                        \
	struct gomp_team_state *ts = &gomp_thread()->ts;                                                                                    \
	int level = ts->level;                                                                                                              \
	int i = level - 1;                                                                                                                  \
        if (i >= 0)                                                                                                                         \
	{                                                                                                                                   \
		for (; i > 0; --i)                                                                                                          \
			ts = &ts->team->prev_ts;                                                                                            \
	}                                                                                                                                   \
	fprintf(stderr, "%ld-%d: %d %s - " format "\n", syscall(SYS_gettid), ts->team_id, level, __FUNCTION__, ##__VA_ARGS__);              \
	fflush(stderr);                                                                                                                     \
} while (0) */


#define MIN(a, b)                  \
({                                 \
	__auto_type __a = a;       \
	__auto_type __b = b;       \
	(__a < __b) ? __a : __b;   \
})


#define MAX(a, b)                  \
({                                 \
	__auto_type __a = a;       \
	__auto_type __b = b;       \
	(__a > __b) ? __a : __b;   \
})


#define CACHE_LINE_SIZE  64


#endif /* MACROS_H */

