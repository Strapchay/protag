package orchestrator

import coordination "aion-coordination"

func coordinationOptions(config *Config, stateDir string) coordination.Options {
	options := coordination.Options{StateDir: stateDir}
	if config == nil {
		return options
	}
	options.MaxNegotiationRounds = config.Coordination.MaxNegotiationRounds
	options.MaxProcessAttempts = config.Coordination.MaxProcessAttempts
	options.MaxRepairAttempts = config.Coordination.MaxRepairAttempts
	options.MaxContextExpansions = config.Coordination.MaxContextExpansions
	return options
}
