## ADDED Requirements

### Requirement: Transport-only activity avoids lifecycle storage reads
Unconfirmed candle updates, transport heartbeats and activity-only connection establishment SHALL update their existing transport facts without initiating lifecycle synchronization or lifecycle snapshot reads. Confirmed candle events SHALL retain synchronization both before ingestion and after their outcome. Subscription, disconnect, recovery and admission scenarios SHALL retain their current synchronization behavior.

#### Scenario: Intermediate candle
- **WHEN** an admitted stream receives an unconfirmed candle update
- **THEN** transport activity is updated without a lifecycle snapshot read, canonical candle mutation or notification.

#### Scenario: Heartbeat or connection establishment
- **WHEN** a heartbeat or an activity-only connected event is observed
- **THEN** its transport facts are updated without lifecycle storage access.

#### Scenario: Confirmed candle
- **WHEN** a confirmed candle is observed and its ingestion produces an outcome
- **THEN** both existing lifecycle synchronization points execute with the existing handling of committed, duplicate, corrected, rejected and failed outcomes.

### Requirement: Periodic synchronization owns time-driven projection refresh
The existing stale worker SHALL continue to evaluate every configured stream and perform its full lifecycle synchronization on every periodic turn, including turns that produce no stale signals. Time-driven changes to the stored readiness projection SHALL be reflected on the next such turn according to the configured interval and event-loop scheduling. Current supervisor readiness and publication predicates SHALL continue to evaluate the expected bar using current time and grace.

#### Scenario: Grace expires between periodic turns
- **WHEN** a required candle or OI frontier falls behind at close plus grace and only transport-only events arrive before the next turn
- **THEN** those events do not refresh lifecycle storage
- **AND** the next periodic turn revokes readiness through the existing stale and lifecycle rules.

#### Scenario: Multiple overdue streams
- **WHEN** several streams become overdue without receiving events
- **THEN** the periodic turn evaluates all of them and refreshes every affected public projection.

#### Scenario: No new stale signal
- **WHEN** stale detection produces no signal for a periodic turn
- **THEN** the full lifecycle and status refresh still executes.

### Requirement: OI observation explicitly refreshes its stream projection
After a live OI observation changes its proven frontier or its recovery/status facts, MDS SHALL explicitly synchronize that stream's lifecycle and public status before the publication eligibility decision. The callback SHALL reuse the existing lifecycle transition rules and SHALL NOT synchronize unrelated streams. Prefetch storage alone SHALL NOT advance the proven frontier.

#### Scenario: OI completes the required data set
- **WHEN** a stored OI observation advances the frontier and makes a stream data-ready
- **THEN** its lifecycle and public projection are refreshed without waiting for another WS event
- **AND** notification eligibility is evaluated with the existing published-through rule.

#### Scenario: OI reveals a discontinuity
- **WHEN** an OI observation requires recovery even though the frontier cannot advance
- **THEN** that stream's recovery requirement is propagated and its lifecycle and projection are synchronized before publication is evaluated.

#### Scenario: Duplicate OI or failed append
- **WHEN** an OI append fails or a duplicate observation changes no frontier or recovery/status facts
- **THEN** no additional lifecycle synchronization is initiated by that observation.

#### Scenario: Stream isolation
- **WHEN** one stream's OI facts change
- **THEN** the explicit OI-triggered lifecycle synchronization reads and updates only that stream.
