/* Runtime-private interface between external-thread support and collectors.
 * These declarations are implementation details, not a supported embedding
 * API, and may change without compatibility guarantees. Hosts call guarded
 * Scala exports instead.
 */
#ifndef SCALANATIVE_EXTERNAL_THREAD_GC_H
#define SCALANATIVE_EXTERNAL_THREAD_GC_H

#include "nativeThreadTLS.h"
#include "gc/shared/ScalaNativeGC.h"

/* Internal collector interface. Callers hold runtime ownership in a
 * single-threaded build. Registration publishes an unmanaged context. */

/* Whether the calling thread has an active collector context. */
bool scalanative_externalGC_isRegistered(void);

/* Register the calling thread as unmanaged; return false on failure. */
bool scalanative_externalGC_register(void);

/* Rebind the collector context to the calling thread's stack; false on failure.
 */
bool scalanative_externalGC_rebind(void);

/* The calling thread's collector context, usable for thread-exit cleanup
 * without updating its stack metadata. */
void *scalanative_externalGC_currentContext(void);
/* Makes the collector observe `info`, a copy of the current thread's
 * ThreadInfo which outlives its compiler TLS. An unmanaged thread waits for
 * a collection in progress, which can still read the previous one. A managed
 * thread is not scanned, call it before leaving managed execution then. */
void scalanative_externalGC_replaceThreadInfo(ThreadInfo *info);

/* Stop tracking the calling thread's stack for collection. */
void scalanative_externalGC_unregister(void);

/* Stop scanning the unmanaged caller's stack until it is admitted. */
void scalanative_externalGC_returnToHost(void);

/* Safely enter managed execution; requires registration. */
void scalanative_externalGC_admit(void);

/* Read the calling thread's managed/unmanaged state; requires registration. */
GC_MutatorThreadState scalanative_externalGC_state(void);

/* Register [low, high) as GC roots; return false on failure. */
bool scalanative_externalGC_addRoots(void *low, void *high);

/* Save a context for thread-exit cleanup using persistent stack metadata.
 * Requires registration and unmanaged execution with a fresh stack snapshot. */
void *scalanative_externalGC_saveContext(ThreadInfo *info);

/* Restore a saved context for same-thread cleanup; return false on failure. */
bool scalanative_externalGC_restoreContext(void *context);

#endif
