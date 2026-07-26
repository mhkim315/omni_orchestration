package coordinator

// EffectiveModelUnverified is used when a provider cannot confirm which
// model actually served the request. This prevents unverified data from
// polluting routing statistics.
const EffectiveModelUnverified = "unverified"

// AgentProfile captures the agent configuration used for a coordinator call.
// Native values are preserved — no forced common enum.
type AgentProfile struct {
	Provider string `json:"provider"` // "codex" | "claude" | "agy" | "reasonix"
	Model    string `json:"model"`    // provider-native model string
	Mode     string `json:"mode"`     // provider-native effort/reasoning value
	Role     string `json:"role"`     // "coordinator" | "worker" | "validator"
}

// RunRecord captures what model was requested vs. what actually served.
type RunRecord struct {
	RequestedModel string `json:"requested_model"` // from AgentProfile
	ResolvedModel  string `json:"resolved_model"`  // provider-reported (or "unverified")
	EffectiveModel string `json:"effective_model"` // confirmed model used (or "unverified")
}

// Capabilities describes what model/effort controls a provider supports.
type Capabilities struct {
	ModelSelection  bool `json:"model_selection"`
	EffortSelection bool `json:"effort_selection"`
	ModelDiscovery  bool `json:"model_discovery"`
	EffectiveModel  bool `json:"effective_model"`
}

// ProviderCapabilities returns the capability set for a named provider.
func ProviderCapabilities(provider string) Capabilities {
	switch provider {
	case "codex":
		return Capabilities{ModelSelection: true, EffortSelection: false, ModelDiscovery: false, EffectiveModel: false}
	case "claude":
		return Capabilities{ModelSelection: true, EffortSelection: true, ModelDiscovery: false, EffectiveModel: false}
	case "agy":
		return Capabilities{ModelSelection: true, EffortSelection: true, ModelDiscovery: true, EffectiveModel: false}
	case "reasonix":
		return Capabilities{ModelSelection: true, EffortSelection: false, ModelDiscovery: false, EffectiveModel: false}
	default:
		return Capabilities{}
	}
}

// DefaultProfile returns the default profile for a named provider.
func DefaultProfile(provider string) AgentProfile {
	switch provider {
	case "codex":
		return AgentProfile{Provider: "codex", Model: "gpt-5.2", Mode: "reasoning_effort", Role: "coordinator"}
	case "claude":
		return AgentProfile{Provider: "claude", Model: "claude-sonnet-4-5-20251001", Mode: "high", Role: "coordinator"}
	case "agy":
		return AgentProfile{Provider: "agy", Model: "claude-sonnet-4-6", Mode: "high", Role: "coordinator"}
	case "reasonix":
		return AgentProfile{Provider: "reasonix", Model: "deepseek-pro/deepseek-v4-pro", Mode: "", Role: "coordinator"}
	default:
		return AgentProfile{Role: "coordinator"}
	}
}

// Profile returns the AgentProfile for a coordinator's current configuration.
func (c *CodexCoordinator) Profile() AgentProfile {
	return AgentProfile{
		Provider: "codex", Model: c.Model, Mode: "reasoning_effort", Role: "coordinator",
	}
}

// Profile returns the AgentProfile for a coordinator's current configuration.
func (c *ClaudeCoordinator) Profile() AgentProfile {
	return AgentProfile{
		Provider: "claude", Model: c.Model, Mode: c.Effort, Role: "coordinator",
	}
}

// Profile returns the AgentProfile for a coordinator's current configuration.
func (c *AGYCoordinator) Profile() AgentProfile {
	return AgentProfile{
		Provider: "agy", Model: c.Model, Mode: c.Effort, Role: "coordinator",
	}
}

// Profile returns the AgentProfile for a coordinator's current configuration.
func (c *ReasonixCoordinator) Profile() AgentProfile {
	return AgentProfile{
		Provider: "reasonix", Model: c.Model, Mode: "", Role: "coordinator",
	}
}

// ApplyProfile sets model/effort from a profile onto the coordinator.
func (c *CodexCoordinator) ApplyProfile(p AgentProfile) {
	if p.Model != "" {
		c.requestedModel = p.Model
		c.Model = p.Model
	}
}

// ApplyProfile sets model/effort from a profile onto the coordinator.
func (c *ClaudeCoordinator) ApplyProfile(p AgentProfile) {
	if p.Model != "" {
		c.requestedModel = p.Model
		c.Model = p.Model
	}
	if p.Mode != "" {
		c.Effort = p.Mode
	}
}

// ApplyProfile sets model/effort from a profile onto the coordinator.
func (c *AGYCoordinator) ApplyProfile(p AgentProfile) {
	if p.Model != "" {
		c.requestedModel = p.Model
		c.Model = p.Model
	}
	if p.Mode != "" {
		c.Effort = p.Mode
	}
}

// ApplyProfile sets model/effort from a profile onto the coordinator.
func (c *ReasonixCoordinator) ApplyProfile(p AgentProfile) {
	if p.Model != "" {
		c.requestedModel = p.Model
		c.Model = p.Model
	}
}

// RecordRun returns the model verification record for a completed run.
// If the provider cannot confirm which model actually served, EffectiveModel
// is "unverified" — never silently assumed to match the requested model.
func (c *CodexCoordinator) RecordRun() RunRecord {
	return RunRecord{
		RequestedModel: c.requestedModel,
		EffectiveModel: EffectiveModelUnverified,
	}
}

// RecordRun returns the model verification record for a completed run.
func (c *ClaudeCoordinator) RecordRun() RunRecord {
	return RunRecord{
		RequestedModel: c.requestedModel,
		EffectiveModel: EffectiveModelUnverified,
	}
}

// RecordRun returns the model verification record for a completed run.
func (c *AGYCoordinator) RecordRun() RunRecord {
	return RunRecord{
		RequestedModel: c.requestedModel,
		EffectiveModel: EffectiveModelUnverified,
	}
}

// RecordRun returns the model verification record for a completed run.
func (c *ReasonixCoordinator) RecordRun() RunRecord {
	return RunRecord{
		RequestedModel: c.requestedModel,
		EffectiveModel: EffectiveModelUnverified,
	}
}
