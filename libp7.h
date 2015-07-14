#ifndef     _LIBP7_H_
#define     _LIBP7_H_

#include    <p7impl.h>

void p7_coro_yield(void);
void p7_coro_create(void (*entry)(void *), void *arg, size_t stack_size);
void p7_timed_event(unsigned long long dt, void (*func)(void *), void *arg, void (*dtor)(void *, void (*)(void *)));
int p7_iowrap_(int fd, int rdwr);
int p7_init(unsigned nthreads);

#define p7_iowrap(_fn_, _rdwr_, _fd_, ...) \
({ \
    int fd_ = (_fd_), rdwr_ = (_rdwr_); \
    p7_iowrap_(fd_, rdwr_); \
    _fn_(fd_, __VA_ARGS__); \
})

#endif      // _LIBP7_H_
