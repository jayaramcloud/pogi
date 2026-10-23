package generic

func init() {
	apiHooks["bigquerydatatransfer"] = hooks{
		newID: map[string]func() string{"TransferConfig": uuid}, // the server names a transfer config
		created: map[string]func(*call, map[string]any){
			// the engine's ready value for state is RUNNING; a new config has had no run yet
			"TransferConfig": func(_ *call, cfg map[string]any) { delete(cfg, "state") },
		},
	}
}
