package coordinator

import (
	"encoding/json"
	"strings"
)

// extractCodexJSONL handles the Codex exec --json output format.
// Codex wraps results in JSONL: {"type":"result","message":{"content":[...]}}
// The actual decision JSON is embedded in the content text.
func extractCodexJSONL(output string) []byte {
	// Try direct parse first (bare JSON).
	if json.Valid([]byte(output)) {
		return []byte(output)
	}
	// Parse each JSONL line, looking for the "result" type with embedded content.
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
		}
		if json.Unmarshal([]byte(line), &wrapper) == nil && wrapper.Type == "result" {
			for _, c := range wrapper.Message.Content {
				if c.Type == "text" && json.Valid([]byte(c.Text)) {
					return []byte(c.Text)
				}
			}
		}
	}
	return extractJSON(output)
}

// extractClaudeCodeBlock handles Claude -p --output-format json.
// Claude may return bare JSON or JSON wrapped in code fences.
func extractClaudeCodeBlock(output string) []byte {
	// Try bare JSON first (Claude --output-format json returns it directly).
	if json.Valid([]byte(output)) {
		return []byte(output)
	}
	// Try code fence extraction.
	if idx := strings.Index(output, "```json"); idx >= 0 {
		start := idx + 7
		if end := strings.Index(output[start:], "```"); end >= 0 {
			block := strings.TrimSpace(output[start : start+end])
			if json.Valid([]byte(block)) {
				return []byte(block)
			}
		}
	}
	// Fall back to generic extraction.
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
