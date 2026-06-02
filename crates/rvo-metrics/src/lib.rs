use std::thread;
use tiny_http::{Response, Server};

mod metrics;
pub use metrics::{render_prometheus, METRICS};

pub fn start_metrics_server(port: u16) {
    thread::spawn(move || {
        let server = Server::http(format!("127.0.0.1:{}", port)).expect("metrics server");

        for req in server.incoming_requests() {
            match req.url() {
                "/metrics" => {
                    let body = render_prometheus();
                    let _ = req.respond(Response::from_string(body));
                }
                "/health" => {
                    // Lightweight liveness probe — always returns 200 while
                    // the process is alive. A richer readiness check (camera
                    // alive, scheduler running) belongs in a future /ready
                    // endpoint.
                    let _ = req.respond(Response::from_string("ok"));
                }
                _ => {
                    let _ = req.respond(Response::from_string("not found").with_status_code(404));
                }
            }
        }
    });
}
