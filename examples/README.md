# Go-Keylane Examples

This directory contains runnable, fully-contained examples demonstrating the core usage patterns of the `go-keylane` library.

---

## 1. Directory Structure

- **[fire_and_forget](fire_and_forget/)**: A basic guide showing how to initialize a queue, submit several light jobs asynchronously, start workers, and perform a graceful teardown.
- **[submit_value](submit_value/)**: Demonstrates the `SubmitValue` and `Await` models to route jobs and block until execution results or computation values are returned to the caller.
- **[business_service](business_service/)**: A realistic simulated enterprise billing payment service incorporating high-priority billing lanes, background webhooks, audit log aggregation, context cancellation check handling, and graceful drain procedures under load.

---

## 2. Running Examples

You can run any example directly from the repository root:

```bash
# Run Fire and Forget Example
go run ./examples/fire_and_forget

# Run Submit Value Example
go run ./examples/submit_value

# Run Business Service Example
go run ./examples/business_service
```
