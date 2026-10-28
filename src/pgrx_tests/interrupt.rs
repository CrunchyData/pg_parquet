#[pgrx::pg_schema]
mod tests {
    use std::{
        io::Write,
        net::{TcpListener, TcpStream},
        process::{Child, Command, Stdio},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{copy_session_output, test_pg_port, COPY_START_TIMEOUT};

    // an object store spends its own timeout and retry budget on a request that is never
    // answered, which is minutes by default, so a wait that only ends with the request is a
    // wait no interruption can shorten. The copies below are pointed at a listener that
    // answers nothing and have to end when they are interrupted instead.
    const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);

    // HangListener accepts connections and answers none of them. The connections are held
    // open rather than dropped, since a closed connection fails the request it carries and
    // the request has to stay in flight for the copy to keep waiting on it.
    struct HangListener {
        port: u16,
        accepted: Arc<AtomicUsize>,
    }

    fn hang_listener() -> HangListener {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("failed to bind the hanging listener");

        let port = listener
            .local_addr()
            .expect("the hanging listener has no address")
            .port();

        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_listener = accepted.clone();

        std::thread::spawn(move || {
            let mut held: Vec<TcpStream> = vec![];

            for connection in listener.incoming() {
                match connection {
                    Ok(connection) => {
                        held.push(connection);
                        accepted_by_listener.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => break,
                }
            }
        });

        HangListener { port, accepted }
    }

    // copy_from_hanging_uri starts a copy that reads a uri nothing answers in a session of
    // its own and returns it once its backend is waiting on the request
    fn copy_from_hanging_uri(listener: &HangListener) -> (Child, i32) {
        let pg_port = test_pg_port();

        let mut copy = Command::new("psql")
            .args(["-h", "localhost", "-p", &pg_port, "-d", "pgrx_tests", "-At"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        let stdin = copy.stdin.as_mut().expect("psql took no input");
        writeln!(stdin, "CREATE TEMP TABLE hanging_copy_table (id bigint);")
            .and_then(|()| {
                writeln!(
                    stdin,
                    "COPY hanging_copy_table FROM 'http://127.0.0.1:{}/hang.parquet'
                     WITH (format parquet);",
                    listener.port
                )
            })
            .and_then(|()| stdin.flush())
            .expect("failed to send the copy to psql");

        let copy_pid = wait_for_hanging_copy(listener);

        (copy, copy_pid)
    }

    // wait_for_hanging_copy returns the pid of the backend running the copy once the listener
    // took its connection, which is the point the backend is inside the wait rather than on
    // its way to it: an interruption that arrives earlier is reported by the copy's own
    // interrupt checks and would prove nothing about the wait
    fn wait_for_hanging_copy(listener: &HangListener) -> i32 {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let copy_pid = Spi::get_one::<i32>(
                "SELECT coalesce(max(pid), 0) FROM pg_stat_activity
                 WHERE query LIKE 'COPY hanging_copy_table FROM%' AND state = 'active'
                   AND pid <> pg_backend_pid()",
            )
            .unwrap()
            .unwrap();

            if copy_pid != 0 && listener.accepted.load(Ordering::SeqCst) > 0 {
                return copy_pid;
            }

            assert!(
                Instant::now() < deadline,
                "the copy did not reach the object store request in {COPY_START_TIMEOUT:?}"
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // wait_for_copy_session waits for the session running the copy to leave the statement,
    // and fails rather than hanging with it when the interruption was ignored
    fn wait_for_copy_session(copy_pid: i32, state: &str) {
        let deadline = Instant::now() + INTERRUPT_TIMEOUT;

        loop {
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let session_state = Spi::get_one::<String>(&format!(
                "SELECT coalesce(max(state), 'gone') FROM pg_stat_activity WHERE pid = {copy_pid}"
            ))
            .unwrap()
            .unwrap();

            if session_state == state {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "the copy waiting on the object store was {session_state} rather than {state} \
                 {INTERRUPT_TIMEOUT:?} after it was interrupted"
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    #[pg_test]
    fn test_copy_from_an_unanswered_object_store_takes_a_cancel() {
        let listener = hang_listener();

        let (copy, copy_pid) = copy_from_hanging_uri(&listener);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend running the copy took no cancel");

        wait_for_copy_session(copy_pid, "idle");

        let output = copy_session_output(copy);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy ended with the request rather than with the cancel: {output}"
        );
    }

    #[pg_test]
    fn test_copy_from_an_unanswered_object_store_takes_a_terminate() {
        let listener = hang_listener();

        let (copy, copy_pid) = copy_from_hanging_uri(&listener);

        let terminated = Spi::get_one::<bool>(&format!("SELECT pg_terminate_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(terminated, "the backend running the copy took no terminate");

        wait_for_copy_session(copy_pid, "gone");

        let output = copy_session_output(copy);

        assert!(
            output.contains("terminating connection due to administrator command"),
            "the copy ended with the request rather than with the terminate: {output}"
        );
    }
}
