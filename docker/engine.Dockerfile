FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY migrations migrations
ARG GIT_SHA=dev
ENV GIT_SHA=${GIT_SHA}
RUN cargo build --release --locked -p polygemy-engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 engine
COPY --from=build /src/target/release/polygemy-engine /usr/local/bin/polygemy-engine
USER engine
ENTRYPOINT ["polygemy-engine"]
CMD ["run"]
