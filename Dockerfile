FROM alpine:3.22.1

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Copy the pre-built binary (goreleaser will provide this).
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/housekeeper /usr/local/bin/
RUN chmod +x /usr/local/bin/housekeeper; \
  adduser -D -u 1000 housekeeper

# Use non-root user for security
USER housekeeper

VOLUME ["/schema"]
ENTRYPOINT ["housekeeper"]
CMD ["--help"]
