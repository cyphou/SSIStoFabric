# =============================================================================
# Multi-stage Dockerfile for SSIStoFabric
# =============================================================================
# Stage 1: builder — installs the package and its dependencies
# Stage 2: runtime — slim image with only the installed package

FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN pip install --no-cache-dir build wheel

# Copy only what's needed for the build
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Build a wheel
RUN python -m build --wheel --outdir /dist

# ---------------------------------------------------------------------------

FROM python:3.12-slim AS runtime

# Non-root user for security
RUN useradd -m -u 1000 ssis2fabric

WORKDIR /app

# Install the built wheel
COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# Switch to non-root
USER ssis2fabric

# Default entrypoint — can be overridden at runtime
ENTRYPOINT ["ssis2fabric"]
CMD ["--help"]
