# lazarus-lib

> **Intelligent exception healing for Spring Boot applications.**  
> Go beyond retry — capture, persist, and reprocess failures with zero boilerplate.

[![Java](https://img.shields.io/badge/Java-17+-orange)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green)](https://spring.io/projects/spring-boot)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## The Problem with Spring Retry

`@Retryable` and Spring Retry are excellent for transient faults — network blips, brief DB unavailability. But they have a fundamental constraint: they retry **synchronously**, in the same thread, within the same request lifecycle. If all retries are exhausted, the exception propagates and **the payload is gone**.

This means:
- Failed business events vanish silently.
- Ops teams have no visibility into *what* failed and *with what data*.
- There is no path to reprocess once the underlying issue is fixed.

`lazarus-lib` treats failures as **durable events**, not transient noise.

---

## What `@Heal` Does Differently

| Feature | Spring `@Retryable` | `@Heal` |
|---|---|---|
| Retry on exception | ✅ | ✅ |
| Configurable backoff | ✅ | ✅ |
| Persists failed payload | ❌ | ✅ |
| Async reprocessing via Kafka | ❌ | ✅ |
| Reprocess via Lambda/REST | ❌ | ✅ |
| Queryable failure history | ❌ | ✅ (via lazarus-mcp) |
| AI-assisted triage | ❌ | ✅ (via lazarus-mcp) |

---

## Quick Start

### 1. Add the dependency

```xml
<!-- Maven -->
<dependency>
  <groupId>io.github.byte-by-k</groupId>
  <artifactId>lazarus-lib</artifactId>
  <version>1.0.0</version>
</dependency>
```

### 2. Enable the Healer

```java
@SpringBootApplication
@EnableExceptionHealer
public class MyApplication { }
```

### 3. Annotate your method

```java
@Service
public class OrderService {

    @Heal(
        retryOn   = { TransientDataException.class, TimeoutException.class },
        backoff   = @HealBackoff(initialDelay = 500, multiplier = 2, maxDelay = 8000),
        retryLimit = 3,
        reprocessVia = ReprocessingStrategy.KAFKA,
        topic     = "healer.reprocess.orders"
    )
    public void processOrder(@HealPayload OrderRequest order) {
        // Your business logic — just write the happy path
        orderRepository.save(order.toEntity());
        paymentGateway.charge(order.getPaymentDetails());
    }
}
```

---

## Core Annotations

### `@Heal`

Placed on any Spring-managed bean method. Activates the Healer interceptor for that method.

| Attribute | Type | Description |
|---|---|---|
| `retryOn` | `Class<? extends Throwable>[]` | Exceptions that trigger healing |
| `retryLimit` | `int` | Max in-process retry attempts before persisting (default: 3) |
| `backoff` | `@HealBackoff` | Backoff policy for in-process retries |
| `reprocessVia` | `ReprocessingStrategy` | `KAFKA`, `LAMBDA`, `REST` |
| `topic` | `String` | Kafka topic (if `KAFKA`) |
| `lambdaArn` | `String` | AWS Lambda ARN (if `LAMBDA`) |
| `endpoint` | `String` | REST URL (if `REST`) |
| `persistent` | `boolean` | Whether to persist to the Healer DB (default: `true`) |

### `@HealPayload`

Marks the single method parameter as the payload to capture on failure. The parameter is serialized to JSON and stored in the `healer_events` table.

```java
// ✅ Correct — one parameter, annotated
public void handleEvent(@HealPayload MyEvent event) { ... }

// ❌ Won't compile — @Heal methods must have exactly one @HealPayload param
public void process(String id, @HealPayload MyEvent event) { ... }
```

### `@HealBackoff`

```java
@HealBackoff(
    initialDelay = 1000,   // ms before first retry
    multiplier   = 2.0,    // exponential multiplier
    maxDelay     = 30000,  // cap at 30s
    jitter       = true    // add ±10% random jitter
)
```

---

## Architecture

```
  ┌─────────────────────────────────────────────────────────┐
  │                    Your Spring App                       │
  │                                                          │
  │   @Service                                               │
  │   processOrder(@HealPayload OrderRequest)                │
  │           │                                              │
  │           ▼                                              │
  │   ┌──────────────────┐                                   │
  │   │  HealerInterceptor│  (AOP Around Advice)             │
  │   │  - catches target │                                   │
  │   │    exceptions     │                                   │
  │   │  - runs backoff   │                                   │
  │   │    retry loop     │                                   │
  │   └────────┬─────────┘                                   │
  │            │ all retries exhausted                        │
  │            ▼                                              │
  │   ┌──────────────────┐                                   │
  │   │  HealerEventStore│  (persists to DB)                 │
  │   │  healer_events   │──────────────────────────────┐    │
  │   │  table           │                              │    │
  │   └────────┬─────────┘                              │    │
  │            │                                        │    │
  │            ▼                                        │    │
  │   ┌──────────────────────────────┐                  │    │
  │   │     ReprocessingRouter       │                  │    │
  │   │  - KAFKA: publish to topic   │                  │    │
  │   │  - LAMBDA: invoke ARN        │                  │    │
  │   │  - REST: POST to endpoint    │                  │    │
  │   └──────────────────────────────┘                  │    │
  └─────────────────────────────────────────────────────┘    │
                                                             │
  ┌──────────────────────────────────────────────────────────┘
  │
  │   healer_events table (PostgreSQL / any JPA datasource)
  │   ┌──────────┬─────────────┬──────────┬──────────────────────┐
  │   │ event_id │ method_name │ status   │ payload_json         │
  │   ├──────────┼─────────────┼──────────┼──────────────────────┤
  │   │ uuid-1   │ processOrder│ PENDING  │ {"orderId":"A1",...} │
  │   │ uuid-2   │ handleInv.. │ RESOLVED │ {"itemId":"B9",...}  │
  │   └──────────┴─────────────┴──────────┴──────────────────────┘
```

---

## Healer DB Schema

```sql
CREATE TABLE healer_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    method_name     VARCHAR(255) NOT NULL,
    class_name      VARCHAR(255) NOT NULL,
    exception_type  VARCHAR(255) NOT NULL,
    exception_msg   TEXT,
    payload_json    JSONB NOT NULL,
    reprocess_via   VARCHAR(50),
    reprocess_target VARCHAR(500),
    status          VARCHAR(50) DEFAULT 'PENDING',
    retry_count     INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX idx_healer_status   ON healer_events(status);
CREATE INDEX idx_healer_method   ON healer_events(method_name);
CREATE INDEX idx_healer_created  ON healer_events(created_at DESC);
```

---

## Reprocessing Strategies

### Kafka (recommended for high-throughput)

The Healer serializes the `@HealPayload` back to its original JSON and publishes it to the configured topic.

```yaml
healer:
  kafka:
    bootstrap-servers: localhost:9092
    producer-config:
      acks: all
      retries: 3
```

### AWS Lambda

Invokes the configured Lambda ARN with the payload as the event body.

```java
@Heal(
    retryOn      = { RemoteCallException.class },
    reprocessVia = ReprocessingStrategy.LAMBDA,
    lambdaArn    = "arn:aws:lambda:us-east-1:123456789012:function:reprocess-orders"
)
```

### REST Endpoint

POSTs the serialized payload to any HTTP endpoint.

```java
@Heal(
    retryOn      = { ServiceUnavailableException.class },
    reprocessVia = ReprocessingStrategy.REST,
    endpoint     = "${healer.reprocess.order-endpoint}"
)
```

---

## Configuration Reference

```yaml
healer:
  enabled: true
  datasource:
    # Uses your Spring Boot datasource by default
  kafka:
    bootstrap-servers: localhost:9092
  lambda:
    region: us-east-1
  polling:
    enabled: true
    interval: PT5M
    batch-size: 100
    dead-letter-after: 5
```

---

## Roadmap

- [ ] `v1.0` — Core `@Heal` interceptor, DB persistence, Kafka reprocessing
- [ ] `v1.1` — Lambda + REST reprocessing strategies
- [ ] `v1.2` — Background polling scheduler
- [ ] `v1.3` — `@HealListener` — declare reprocessing consumers inline
- [ ] `v2.0` — `lazarus-mcp` integration — AI agent can query, triage, and resolve events
- [ ] `v2.1` — Dashboard UI (Spring Boot Actuator endpoint + simple HTML view)
- [ ] `v2.2` — Multi-tenancy support

---

## Related Projects

- **[lazarus-mcp](https://github.com/byte-by-k/lazarus-mcp)** — MCP Server exposing the Healer DB to AI agents
- **[mcp-agent-java](https://github.com/byte-by-k/mcp-agent-java)** — Java AI agent that uses lazarus-mcp
- **[mcp-agent-python](https://github.com/byte-by-k/mcp-agent-python)** — Python AI agent with MCP support

---

## License

MIT © Kamlesh
