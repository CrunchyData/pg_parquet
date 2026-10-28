use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::time::timeout;

/// A current-thread tokio runtime, so the reactor runs on the thread that is running the
/// Postgres backend rather than on one of its own. Every entry point in this crate is
/// blocking; the asynchrony is an implementation detail of the `object_store` clients and
/// stops at this boundary.
///
/// There is exactly one of these per loaded copy of the crate, which is the main reason the
/// `pg_object_store` extension publishes a vtable: a cluster that loads three extensions that
/// each vendored this crate gets three runtimes and three credential caches.
pub static OBJECT_STORE_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("failed to create the object store tokio runtime: {e}"))
});

pub fn runtime() -> &'static Runtime {
    &OBJECT_STORE_RUNTIME
}

/// How often a blocked call comes back out of the runtime to give the host a chance to stop
/// the statement. A request that is making progress pays one runtime entry per interval.
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// What the host does at a poll boundary: nothing, or stop the statement.
///
/// Without one of these a blocked request is unstoppable. A store that accepts a connection
/// and then never answers leaves the backend inside a single `block_on` forever, where no
/// statement timeout fires and no cancellation lands, because neither is a signal the thread
/// is in a position to notice — and killing the backend is then the only way out.
static INTERRUPT_SINK: AtomicUsize = AtomicUsize::new(0);

/// Installs the host's interrupt check. It may stop the statement however the host stops one,
/// including by a `longjmp` that never returns, which is why it is only ever called from
/// outside the runtime.
pub fn set_interrupt_sink(sink: fn()) {
    INTERRUPT_SINK.store(sink as usize, Ordering::Relaxed);
}

fn interrupt_check() -> Option<fn()> {
    let sink = INTERRUPT_SINK.load(Ordering::Relaxed);

    // SAFETY: the only writer is set_interrupt_sink, which only ever stores a fn().
    (sink != 0).then(|| unsafe { std::mem::transmute::<usize, fn()>(sink) })
}

/// Runs a future to completion on the shared runtime, letting the host stop the statement
/// while it waits.
///
/// The future is driven in slices and the check is made *between* them, with the runtime left
/// each time. That is the whole point of the shape: the host stops a statement by longjmping
/// out of this frame, and doing that from inside `block_on` would leave the runtime marked as
/// entered on this thread, so the next call on the same backend would find it unusable. The
/// cost of being stopped is the unfinished request: its future is abandoned rather than
/// dropped, since a longjmp runs no destructors, so the connection it held goes with the
/// statement instead of back to the pool.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let Some(check) = interrupt_check() else {
        return OBJECT_STORE_RUNTIME.block_on(future);
    };

    let mut future = Box::pin(future);

    loop {
        let slice = OBJECT_STORE_RUNTIME
            .block_on(async { timeout(INTERRUPT_POLL_INTERVAL, &mut future).await });

        if let Ok(output) = slice {
            return output;
        }

        check();
    }
}

/// Where a trace message goes. A library has no business deciding that, so the host installs
/// a sink. `pg_object_store` points this at `ereport(DEBUG2)`; a test binary points it at
/// stderr; left unset, tracing costs a relaxed load of a null pointer.
static TRACE_SINK: AtomicUsize = AtomicUsize::new(0);

pub fn set_trace_sink(sink: fn(&str)) {
    TRACE_SINK.store(sink as usize, Ordering::Relaxed);
}

pub fn trace(message: impl FnOnce() -> String) {
    let sink = TRACE_SINK.load(Ordering::Relaxed);

    if sink != 0 {
        // SAFETY: the only writer is set_trace_sink, which only ever stores a fn(&str).
        let sink: fn(&str) = unsafe { std::mem::transmute(sink) };
        sink(&message());
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{block_on, set_interrupt_sink};

    thread_local! {
        /// How many poll boundaries are left before the test's sink stops the "statement".
        /// Thread-local so that the sink is inert on any other thread that blocks on the
        /// shared runtime while this test holds it.
        static BOUNDARIES_LEFT: Cell<usize> = const { Cell::new(0) };
    }

    /// Stands in for the host's interrupt check, panicking where that one would longjmp.
    fn stop_at_the_third_boundary() {
        let left = BOUNDARIES_LEFT.get();

        if left == 0 {
            return;
        }

        BOUNDARIES_LEFT.set(left - 1);

        assert!(left > 1, "the statement is stopped");
    }

    #[test]
    fn test_a_blocked_call_comes_back_out_to_be_stopped() {
        set_interrupt_sink(stop_at_the_third_boundary);
        BOUNDARIES_LEFT.set(3);

        // a store that accepts a connection and never answers is this future: before the
        // slicing, the backend sat in here with no way out but being killed
        let stopped = std::panic::catch_unwind(|| block_on(std::future::pending::<()>()));

        set_interrupt_sink(|| {});

        assert!(stopped.is_err(), "the call was not stopped");
        assert_eq!(
            BOUNDARIES_LEFT.get(),
            0,
            "the check was not made every slice"
        );

        // and the runtime is still usable on this thread, which is why the check is made from
        // outside it: being stopped from within would leave it entered
        assert_eq!(block_on(async { 7 }), 7);
    }
}
