FROM alpine@sha256:4562b419adf48c5f3c763995d6014c123b3ce1d2e0ef2613b189779caa787192

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Copy the pre-built binary (goreleaser will provide this).
COPY housekeeper /usr/local/bin/housekeeper
RUN chmod +x /usr/local/bin/housekeeper

# Use non-root user for security
RUN adduser -D -u 1000 housekeeper
USER housekeeper

VOLUME ["/schema"]
ENTRYPOINT ["housekeeper"]
CMD ["--help"]
