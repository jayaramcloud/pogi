package generic

// VPC Lattice: ARNs name their kind in one word (servicenetwork/sn-..., not service-network/...). Nothing routes traffic.
func init() {
	arnTemplates["vpc-lattice/ServiceNetwork"] = "servicenetwork/{serviceNetworkIdentifier}"
	arnTemplates["vpc-lattice/Service"] = "service/{serviceIdentifier}"
	arnTemplates["vpc-lattice/ResourceGateway"] = "resourcegateway/{resourceGatewayIdentifier}"
	arnTemplates["vpc-lattice/TargetGroup"] = "targetgroup/{targetGroupIdentifier}"
	// names are unique in an account and region; the engine would mint a second id
	for op, typ := range map[string]string{"CreateServiceNetwork": "ServiceNetwork", "CreateService": "Service", "CreateResourceGateway": "ResourceGateway", "CreateTargetGroup": "TargetGroup"} {
		Override("vpc-lattice", op, func(c *Call) (map[string]any, error) {
			for _, r := range c.List(typ) {
				if str(r.Data["name"]) == str(c.In["name"]) {
					return nil, c.Exists("%s %s already exists", typ, str(c.In["name"]))
				}
			}
			return c.Default()
		})
	}
}
