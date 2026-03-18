### STAGE 1 ###

# Build the REST API using the official Golang base image
FROM golang:1.24-alpine AS build

# Add Golang specific environment variables for compiling
ENV GOOS=linux GOARCH=amd64

# Enter /api as current working directory
# The commands below will respect this
WORKDIR /app

# Copy rest of the files to working directory
# This is done separately so Docker can use its cache effectively
COPY . .

# Compile the application to a single binary called 'server'
RUN go build -ldflags="-w -s" -o TradesMarket_app ./cmd/TradesMarket

### STAGE 2 ###

# Run the REST API using the official Alpine Linux base image
FROM golang:1.24-alpine AS runtime

WORKDIR /go

# We don't want to run our container as the root user for security reasons
# Therefore, we define a new non-root user and UID for it
# We disable login via password and omit creating a home directory
# to protect us against malicious SSH login attempts
ENV USER=gouser UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

# Copy the compiled binary from 'build' stage
# Binary permissions are given to our custom user to make the app runnable
COPY --from=build --chown=${USER}:${USER} /app/TradesMarket_app ./bin/
# Switch to our new user
USER ${USER}:${USER}

# Define a Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep TradesMarket_app > /dev/null || exit 1

# Launch the application
CMD [ "./bin/TradesMarket_app" ]