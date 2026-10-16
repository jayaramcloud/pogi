package arm

// The tests store resources of a made-up type with no Provider, so the type has to be one pogi knows.
func init() {
	builtin["Microsoft.Widgets"] = []string{"widgets", "widgets/parts"}
}
