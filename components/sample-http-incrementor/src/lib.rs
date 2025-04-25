#![no_main]

use exports::wasi::http::incoming_handler::Guest;
use wasi::http::types::{
    ErrorCode, Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};
use wasi::keyvalue::{
    atomics,
    store::{self, Error},
};

#[derive(Debug, Clone)]
struct SampleHttpIncrementor;

impl SampleHttpIncrementor {
    fn increment(path: &str) -> Result<i64, Error> {
        let bucket = store::open("http-incrementor")?;
        atomics::increment(&bucket, path, 1)
    }
}

impl Guest for SampleHttpIncrementor {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let headers = Fields::new();
        let response = OutgoingResponse::new(headers);
        let body = response.body().unwrap();

        let path_with_query = request.path_with_query().unwrap();
        let parts: Vec<&str> = path_with_query.splitn(2, "?").collect();
        let path = *parts.get(0).unwrap();

        match Self::increment(path) {
            Ok(count) => {
                ResponseOutparam::set(response_out, Ok(response));
                let out = body.write().expect("outgoing stream");
                out.blocking_write_and_flush(format!("{}\n", count).as_bytes())
                    .expect("writing response");
            }
            Err(err) => {
                ResponseOutparam::set(
                    response_out,
                    Err(ErrorCode::InternalError(Some(err.to_string()))),
                );
            }
        }

        OutgoingBody::finish(body, None).unwrap();
    }
}

wit_bindgen::generate!({
    world: "sample-http-incrementor",
    path: "../wit",
    generate_all
});

export!(SampleHttpIncrementor);
