package coordinator

import (
	"encoding/json"
	"strings"
)

// extractCodexJSONL handles the Codex exec --json output format.
// Codex wraps results in JSONL with two formats:
//  1. {"type":"result","message":{"content":[{"type":"text","text":"{...}"}]}}
//  2. {"type":"item.completed","item":{"type":"agent_message","text":"{...}"}}
func extractCodexJSONL(output string) []byte {
	// Try direct parse first (bare JSON).
	if json.Valid([]byte(output)) {
		return []byte(output)
	}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var wrapper struct {
			Type    string `json:"type"`
			Message struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
			Item struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
		}
		if json.Unmarshal([]byte(line), &wrapper) != nil {
			continue
		}
		// Format 1: result wrapper.
		if wrapper.Type == "result" {
			for _, c := range wrapper.Message.Content {
				if c.Type == "text" && json.Valid([]byte(c.Text)) {
					return []byte(c.Text)
				}
			}
		}
		// Format 2: item.completed with agent_message.
		if wrapper.Type == "item.completed" && wrapper.Item.Type == "agent_message" {
			if json.Valid([]byte(wrapper.Item.Text)) {
				return []byte(wrapper.Item.Text)
			}
		}
	}
	return extractJSON(output)
}

// extractClaudeCodeBlock handles Claude -p --output-format json.
// Claude may return:
//  1. Bare JSON: {"decision":"VALIDATE",...}
//  2. Result wrapper: {"result":"{\\"decision\\":\\"VALIDATE\\",...}"}
//  3. Code fence: ```json {...} ```
func extractClaudeCodeBlock(output string) []byte {
	// Try top-level result wrapper FIRST (before bare JSON).
	// {"result":"{\\"decision\\":...}"} is valid JSON — bare check
	// would return the wrapper, not the inner decision.
	var wrapper struct {
		Result string `json:"result"`
	}
	if json.Unmarshal([]byte(output), &wrapper) == nil && wrapper.Result != "" {
		unescaped := strings.ReplaceAll(wrapper.Result, `\"`, `"`)
		if json.Valid([]byte(unescaped)) {
			return []byte(unescaped)
		}
	}
	// Try bare JSON.
	if json.Valid([]byte(output)) {
		return []byte(output)
	}
	// Try code fence.
	if idx := strings.Index(output, "```json"); idx >= 0 {
		start := idx + 7
		if end := strings.Index(output[start:], "```"); end >= 0 {
			block := strings.TrimSpace(output[start : start+end])
			if json.Valid([]byte(block)) {
				return []byte(block)
			}
		}
	}
	return extractJSON(output)
}

// extractAGYPrint handles agy --print output.
// AGY returns bare JSON with no surrounding text.
func extractAGYPrint(output string) []byte {
	return extractJSON(output)
}

// extractReasonixCommentary handles reasonix output with thinking text.
// Reasonix may include thinking/analysis before the JSON decision.
func extractReasonixCommentary(output string) []byte {
	// Try code fence first.
	if idx := strings.Index(output, "```json"); idx >= 0 {
		start := idx + 7
		if end := strings.Index(output[start:], "```"); end >= 0 {
			block := strings.TrimSpace(output[start : start+end])
			if json.Valid([]byte(block)) {
				return []byte(block)
			}
		}
	}
	// Try direct parse.
	if json.Valid([]byte(output)) {
		return []byte(output)
	}
	// Fall back to generic last-JSON extraction.
	return extractJSON(output)
}
