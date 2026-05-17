package core

type lifecycleState uint8

const (
	stateNew lifecycleState = iota
	stateRunning
	stateStopping
	stateStopped
)
