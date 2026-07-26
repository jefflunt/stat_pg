# Design: Connect LucidChart MCP Server

This document details the configuration and integration of the official LucidChart Model Context Protocol (MCP) server into Antigravity.

---

## 1. Goal Description

LucidChart provides an official remote MCP server interface at `https://mcp.lucid.app/mcp`. Integrating this MCP server with Antigravity allows AI agents to interact with Lucid document APIs, search diagrams, and generate visual models.

---

## 2. Proposed Changes

### Configuration
Update `~/.gemini/antigravity/mcp_config.json` to include the `lucid` MCP server entry using the `mcp-remote@latest` package via `npx`:

```json
"lucid": {
  "command": "npx",
  "args": [
    "-y",
    "mcp-remote@latest",
    "https://mcp.lucid.app/mcp"
  ]
}
```

---

## 3. Verification Plan

* Validate JSON syntax of `~/.gemini/antigravity/mcp_config.json`.
* Verify documentation alignment across `agent_docs`.
