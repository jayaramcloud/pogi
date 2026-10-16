package lambda

import "strings"

// RuntimeImage is runtimeImage, for the tests.
var RuntimeImage = runtimeImage

// Environments counts a function's live environments and the idle ones among them.
func (s *Service) Environments(account, region, name string) (live, idle int) {
	prefix := key(account, region, name) + ":"
	s.pool.mu.Lock()
	defer s.pool.mu.Unlock()
	for in := range s.pool.all {
		if strings.HasPrefix(in.spec.key, prefix) {
			live++
		}
	}
	for k, list := range s.pool.idle {
		if strings.HasPrefix(k, prefix) {
			idle += len(list)
		}
	}
	return live, idle
}
