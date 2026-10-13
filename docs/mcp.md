# MCP Server

`tele-mess-core` exposes the archive to AI agents through a read-only
[Model Context Protocol](https://modelcontextprotocol.io) endpoint on the
existing HTTP server:

```text
POST http://<server.host>:<server.port>/mcp
```

Agents such as Claude Code or Codex can then use the archived Telegram messages,
the AI-written daily reports, and the extracted message points as an
information source. Every tool only reads; nothing in the MCP surface can change
accounts, origins, policies, or stored content, and account auth state, phone
numbers, and session paths are never returned.

The endpoint is served by `run-server`, `serve-api`, and `run-local --web`. It
needs no extra process and no extra dependency.

## Tokens

`/mcp` accepts the same two header forms as the rest of the API:

```text
Authorization: Bearer <token>
X-Api-Token: <token>
```

Two tokens are accepted there:

- `server.token`, the management token.
- `server.mcp_token`, an optional second token that is valid **only** on `/mcp`.

Give agents `server.mcp_token`. The management token can add accounts, delete
origins, and trigger Telegram logins; a token that is rejected everywhere except
the read-only MCP endpoint cannot.

```yaml
server:
  host: "127.0.0.1"
  port: 8765
  token: "replace-with-a-long-random-token"
  mcp_token: "replace-with-another-long-random-token"
```

Generate a value with, for example:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Restart the server after changing the config. When `server.mcp_token` is unset,
only `server.token` works on `/mcp`. An MCP token still reads message content,
so treat it as a secret: keep it in an environment variable or a secret store,
not in a committed file. The built-in server has no TLS; for agents on other
machines, bind to a trusted LAN or Tailscale address or use a TLS reverse proxy,
as described in [Server mode](server-mode.md).

## Tools

| Tool | Use it to |
| --- | --- |
| `overview` | Learn what the archive holds: accounts, message count and time range, chat counts, tags, daily report dates and types, message point dates, timezone. Call it first. |
| `list_sources` | Find chats (groups, channels, forum topics, private chats) and their `account_id` + `origin_id`. Filters: `query`, `tag`, `important_only`, `enabled_only`, `account_id`. |
| `search_messages` | Search raw message text by keyword, newest first. Filters: chat, topic, `sender`, `date_from`, `date_to`. |
| `read_messages` | Read raw messages in time order, either for a time window (`hours` or `date_from`/`date_to`, across all chats or one) or as context around one message (`around_message_id`). |
| `list_daily_reports` | List daily reports (`important_daily`, `important_origin`, `point_daily`, older `tag_group`) without their text. |
| `read_daily_report` | Read one report's Markdown by `summary_id`, or by `date` and `record_type`. |
| `search_points` | Search message points: AI-extracted facts, events, decisions, resources, and risks with importance scores and source links. |

Full parameter descriptions are part of each tool's `inputSchema` and are
returned by `tools/list`.

### Behavior worth knowing

- **Search is substring matching.** `search_messages` splits the query on
  whitespace and requires every keyword as a substring (`LIKE`; ASCII letters
  match case-insensitively). The full-text index treats a run of Chinese
  characters, including a Latin word written inside it such as `用claude写`, as
  one token, so substring matching is the default for every query. It reads the
  archive in time order, so without `date_from` or a chat filter it covers only
  the last 90 days; the result states the range that was searched and how to
  widen it.
- **Whole-word search.** `whole_word: true` uses the full-text index instead:
  ASCII keywords match as whole words over the entire archive, with no default
  time window. It keeps a short word from matching inside longer ones, but it
  misses words glued to Chinese text and is rejected for non-ASCII keywords.
- **Deleted messages** are left out unless `include_deleted` is set; asking
  for context around a deleted message without it is an error that says so.
- **Dates.** Plain `YYYY-MM-DD` dates and date-times without an offset are read
  in the daily schedule timezone (reported by `overview`). Returned message
  timestamps use that timezone too.
- **Chat scope.** In the message tools, `origin_id` must be accompanied by
  `account_id`, and `topic_id` by both; `list_sources` returns them together.
  Messages report the chat as `origin_id`.
- **Paging.** Every tool has a `limit`. When a result is cut, `has_more` is true
  and the result says how to continue: `next_cursor` for the message tools,
  `next_offset` for `search_points` and `read_daily_report`, or a note to narrow
  the filters for the list tools. A page of messages or points also ends early
  once it reaches about 24,000 characters, so one tool result stays within what
  MCP clients accept. Context around a message is cut from its outer ends under
  the same budget and marked `truncated`; `limit` does not apply there.
- **Compact output.** Tool results are a single JSON text block. Empty fields
  are omitted, and raw Telegram JSON, reactions, and ingestion bookkeeping are
  never included.

## Connecting clients

The examples use `http://127.0.0.1:8765/mcp` and read the token from the
`TELE_MESS_MCP_TOKEN` environment variable. Replace the host with the server's
LAN or Tailscale address for agents on other machines.

### Claude Code

```bash
claude mcp add --transport http tele-mess-core http://127.0.0.1:8765/mcp \
  --header "Authorization: Bearer ${TELE_MESS_MCP_TOKEN}"
```

The shell expands the variable when the command runs. To keep the token out of
the stored configuration, use a project `.mcp.json` instead; Claude Code expands
`${VAR}` there at startup:

```json
{
  "mcpServers": {
    "tele-mess-core": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp",
      "headers": {
        "Authorization": "Bearer ${TELE_MESS_MCP_TOKEN}"
      }
    }
  }
}
```

### Codex

Add to `~/.codex/config.toml`:

```toml
[mcp_servers.tele-mess-core]
url = "http://127.0.0.1:8765/mcp"
bearer_token_env_var = "TELE_MESS_MCP_TOKEN"
```

### Other clients

Any client that speaks MCP Streamable HTTP can connect with a URL and one
header. The common JSON form is:

```json
{
  "mcpServers": {
    "tele-mess-core": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp",
      "headers": {
        "Authorization": "Bearer ${TELE_MESS_MCP_TOKEN}"
      }
    }
  }
}
```

Clients that do not expand environment variables need the literal token in
their own secret storage. `X-Api-Token: <token>` works where a client cannot set
an `Authorization` header.

### Checking the endpoint by hand

```bash
curl -s http://127.0.0.1:8765/mcp \
  -H "Authorization: Bearer ${TELE_MESS_MCP_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"overview","arguments":{}}}'
```

## Protocol details

The endpoint implements the stateless subset of MCP Streamable HTTP with
hand-written JSON-RPC 2.0:

- Methods: `initialize`, `ping`, `tools/list`, `tools/call`. Notifications such
  as `notifications/initialized` return `202 Accepted` with no body.
- Protocol versions `2025-06-18`, `2025-03-26`, and `2024-11-05` are echoed back
  when requested; any other request is answered with `2025-06-18`.
- Every response is `application/json`. There is no SSE stream, no
  `Mcp-Session-Id`, and no stdio transport; `GET /mcp` and `DELETE /mcp` return
  `405`. Each request carries one JSON-RPC message; batches are rejected.
- Only the `tools` capability is declared, and every tool carries
  `annotations.readOnlyHint: true`.
- Unknown methods return JSON-RPC error `-32601`, an unknown tool or malformed
  `params` returns `-32602`, and unparseable JSON returns `-32700`. Invalid tool
  arguments and missing objects are tool results with `isError: true` and a
  message the agent can act on.

`GET /manage/capabilities` advertises the endpoint, protocol versions, and tool
names under `mcp`.
