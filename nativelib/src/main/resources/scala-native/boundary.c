#if defined(__SCALANATIVE_BOUNDARY_OPT)

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include "gc/shared/ThreadUtil.h"

#if defined(__aarch64__) && !defined(_WIN64)
#define ASM_JMPBUF_SIZE 192
#elif defined(__x86_64__) &&                                                   \
    (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) ||       \
     defined(__OpenBSD__) || defined(__NetBSD__))
#define ASM_JMPBUF_SIZE 72
#elif defined(__i386__) &&                                                     \
    (defined(__linux__) || defined(__APPLE__))
#define ASM_JMPBUF_SIZE 32
#elif defined(__x86_64__) && defined(_WIN64)
#define ASM_JMPBUF_SIZE 256
#else
#error "Unsupported platform for boundary fast path"
#endif

#if defined(__APPLE__)
#define _lh_setjmp lh_setjmp
#define _lh_longjmp lh_longjmp
#endif

typedef void *lh_jmp_buf[ASM_JMPBUF_SIZE / sizeof(void *)];

extern int _lh_setjmp(lh_jmp_buf buf);
extern void *_lh_longjmp(lh_jmp_buf buf, int arg);

typedef struct BoundaryFrame {
    void *label;
    void *result;
    struct BoundaryFrame *next;
    lh_jmp_buf buf;
} BoundaryFrame;

static SN_ThreadLocal BoundaryFrame *current_boundary = NULL;

void scalanative_boundary_push(BoundaryFrame *frame, void *label) {
    frame->label = label;
    frame->result = NULL;
    frame->next = current_boundary;
    current_boundary = frame;
}

void scalanative_boundary_pop(BoundaryFrame *frame) {
    assert(current_boundary == frame);
    current_boundary = frame->next;
}

bool scalanative_boundary_setjmp(BoundaryFrame *frame) {
    return _lh_setjmp(frame->buf) != 0;
}

void *scalanative_boundary_result(BoundaryFrame *frame) {
    return frame->result;
}

bool scalanative_boundary_break_fast(void *label, void *value) {
    BoundaryFrame *frame = current_boundary;
    while (frame != NULL) {
        if (frame->label == label) {
            frame->result = value;
            current_boundary = frame;
            _lh_longjmp(frame->buf, 1);
#if defined(__GNUC__) || defined(__clang__)
            __builtin_unreachable();
#endif
        }
        frame = frame->next;
    }
    return false;
}

#endif
