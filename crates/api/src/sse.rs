//! Server-Sent Events stream for live job-status updates.
//!
//! Implemented by polling the job row on a fixed interval and emitting a
//! `status` event only when the status changes, ending when the job reaches a
//! terminal state (or after a hard cap to bound connection lifetime). Polling
//! keeps the implementation backend-agnostic; `LISTEN/NOTIFY` push is a
//! documented roadmap optimization.

use axum::response::sse::{Event, KeepAlive, Sse};
use djq_core::JobStatus;
use djq_queue::QueueService;
use futures::Stream;
use serde::Serialize;
use std::convert::Infallible;
use std::time::Duration;
use uuid::Uuid;

const POLL: Duration = Duration::from_millis(500);
const MAX_POLLS: u32 = 1200; // ~10 minutes at 500ms

#[derive(Serialize)]
struct JobEvent {
    id: Uuid,
    status: String,
    attempts: i32,
    last_error: Option<String>,
}

struct StreamState {
    service: QueueService,
    id: Uuid,
    last: Option<JobStatus>,
    first: bool,
    done: bool,
    polls: u32,
}

/// Build an SSE response that streams status transitions for a single job.
pub fn job_event_stream(
    service: QueueService,
    id: Uuid,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let state = StreamState {
        service,
        id,
        last: None,
        first: true,
        done: false,
        polls: 0,
    };

    let stream = futures::stream::unfold(state, move |mut st| async move {
        if st.done {
            return None;
        }
        loop {
            if st.polls >= MAX_POLLS {
                st.done = true;
                let ev = Event::default().event("timeout").data("stream closed");
                return Some((Ok(ev), st));
            }
            st.polls += 1;

            match st.service.get_job(st.id).await {
                Ok(job) => {
                    let changed = st.first || st.last != Some(job.status);
                    if changed {
                        st.first = false;
                        st.last = Some(job.status);
                        if job.status.is_terminal() {
                            st.done = true;
                        }
                        let payload = JobEvent {
                            id: job.id,
                            status: job.status.to_string(),
                            attempts: job.attempts,
                            last_error: job.last_error.clone(),
                        };
                        let ev = Event::default()
                            .event("status")
                            .json_data(payload)
                            .unwrap_or_else(|_| {
                                Event::default().event("error").data("encode error")
                            });
                        return Some((Ok(ev), st));
                    }
                    tokio::time::sleep(POLL).await;
                }
                Err(_) => {
                    st.done = true;
                    let ev = Event::default().event("error").data("job not found");
                    return Some((Ok(ev), st));
                }
            }
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
