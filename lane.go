package keylane

// Lane identifies a processing lane with its own quota and queue.
type Lane string

// Validate ensures the lane name is not empty.
func (l Lane) Validate() error {
	if l == "" {
		return ErrInvalidLane
	}
	return nil
}
