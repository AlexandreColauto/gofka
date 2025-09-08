FROM alpine:latest

# It's good practice to run as a non-root user.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Set the working directory for the final image.
WORKDIR /app

# Copy the compiled server executable from the builder stage.
COPY visualizer_server .

COPY static ./static

# Copy your YAML configuration file.
COPY gofka.yaml .

# Expose the port your server listens on.
EXPOSE 42069

# The command to run when the container starts.
CMD ["./visualizer_server"]

