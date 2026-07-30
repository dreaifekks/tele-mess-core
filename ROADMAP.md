# Roadmap

`tele-mess-core` is a local-first Telegram information-management backend. Its
purpose is not to collect the largest possible archive. It helps an operator
follow selected groups, channels, topics, and private conversations without
continuously checking every chat.

The core maintains the selected information sources, preserves searchable
context, organizes incoming content, produces source-grounded summaries, and
delivers the useful result to a chosen Telegram destination. Success means
spending less attention on repetitive group traffic while still being able to
find and verify important information.

## Product Direction

Development is organized around three connected tracks:

1. **Information acquisition and maintenance.** Make account, origin, topic,
   capture-policy, catch-up, media, and runtime-health management reliable and
   observable.
2. **Content organization and summarization.** Improve filtering, grouping,
   deduplication, prioritization, source grounding, summary formats, and
   delivery quality.
3. **Operational usability.** Make first-run setup, policy editing, scheduling,
   job monitoring, error recovery, and result review understandable without
   requiring direct database or configuration-file work.

## Now

- Harden long-running ingestion, reconnect catch-up, bounded backfill, edit and
  delete handling, and per-account failure isolation.
- Make account authentication, origin discovery, topic selection, capture
  policies, participant refresh, and operation failures easier to manage from
  the built-in console and API.
- Improve daily package, summary, message-point, schedule, progress,
  cancellation, retry, history, and Telegram-delivery management as one
  coherent workflow.
- Keep every generated report and message point traceable to its source time,
  origin, tags, Telegram link, and retained archive context.
- Improve first-run setup and safe defaults so operators can start with a small,
  deliberate set of information sources instead of capturing everything.
- Add synthetic fixtures and regression tests for acquisition, organization,
  summary, and delivery behavior without committing private Telegram data.

## Next

- Add stronger content grouping and deduplication so repeated forwards,
  announcements, and cross-posts do not consume repeated attention.
- Make relevance and priority configurable by account, origin, topic, tag, and
  content type, with clear reasons for why an item was included or omitted.
- Support distinct summary modes such as an important-only report, concise daily
  digest, topic recap, chronological catch-up, and action or event list.
- Build repeatable summary-quality evaluation around factual grounding,
  important-item coverage, redundancy, readability, and source-link accuracy.
- Add lightweight feedback controls such as useful, less like this, mute,
  follow, or promote so future organization can better match the operator's
  actual information needs.
- Improve delivery controls for cadence, quiet hours, account, peer, forum
  topic, formatting, retry state, and failure notification.
- Provide onboarding and preview flows that show what a capture or summary rule
  will include before it is enabled.

## Later

- Track continuing topics, events, and unresolved items across multiple days
  instead of treating every daily package as isolated.
- Offer reusable organization and summary presets for different source types,
  such as announcement channels, technical groups, communities, and private
  working groups.
- Add attention-oriented observability: source volume, repeated-content
  reduction, summary coverage, delivery reliability, and the amount of content
  that required manual review.
- Broaden desktop and service-management integrations while keeping the
  archive, policies, jobs, and generated results owned by the core.
- Improve user-owned export and portability for source records, organized
  points, reports, and delivery history.

## Product Principles

- Capture is explicit and policy-driven; more data is not automatically better.
- Summaries reduce noise but never hide their provenance or prevent drill-down.
- Operators control which sources are analyzed, which provider receives
  content, when work runs, and where results are delivered.
- Acquisition and delivery failures must be visible and recoverable rather than
  silently creating gaps.
- The default experience should interrupt the operator less, not create another
  dashboard that demands constant attention.

## Non-goals

- A hosted service that receives operators' Telegram credentials or archives.
- Multi-tenant account management in the initial product.
- Forwarding every source message into backup Telegram groups.
- Maximizing notification volume, engagement time, or the number of captured
  groups.
- Hiding provider output or source provenance behind an opaque summary.

## Contributing

Issues and pull requests are welcome. Start with
[CONTRIBUTING.md](CONTRIBUTING.md), avoid including real Telegram data or
credentials in reports, and use [SECURITY.md](SECURITY.md) for sensitive
findings.
