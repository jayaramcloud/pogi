package generic

func init() {
	// the server names a job; a job template is named by the client in the query
	apiHooks["transcoder"] = hooks{newID: map[string]func() string{"Job": uuid}}
}
