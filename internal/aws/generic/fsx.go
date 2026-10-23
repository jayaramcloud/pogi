package generic

// FSx: file systems are stored and are AVAILABLE at once, with a DNSName. Nothing can be mounted. Lustre and
// OpenZFS get the members their clients read (the mount name, the root volume). The VPC is read from pogi's EC2
// when it knows the first subnet. Windows and ONTAP file systems are stored from the request as it came.
func init() {
	arnTemplates["fsx/FileSystem"] = "file-system/{FileSystemId}"
	Override("fsx", "CreateFileSystem", func(c *Call) (map[string]any, error) {
		id := "fs-" + randHex(17)
		c.In["FileSystemId"] = id
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		d := map[string]any{"Lifecycle": "AVAILABLE", "FileSystemId": id, "DNSName": id + ".fsx." + c.Region + ".amazonaws.com", "OwnerId": c.Account,
			"NetworkInterfaceIds": []any{}}
		if c.In["StorageType"] == nil {
			d["StorageType"] = "SSD"
		}
		if ids := asList(c.In["SubnetIds"]); len(ids) > 0 {
			var sn struct {
				VPC string `xml:"subnetSet>item>vpcId"`
			}
			if ec2Describe(c, "DescribeSubnets", "SubnetId", ids[:1], &sn) && sn.VPC != "" {
				d["VpcId"] = sn.VPC
			}
		}
		switch str(c.In["FileSystemType"]) {
		case "LUSTRE":
			l := s2Clone(s2Map(c.In["LustreConfiguration"]))
			if l["DeploymentType"] == nil {
				l["DeploymentType"] = "SCRATCH_1"
			}
			l["MountName"] = randHex(8)
			d["LustreConfiguration"] = l
		case "OPENZFS":
			z := s2Clone(s2Map(c.In["OpenZFSConfiguration"]))
			z["RootVolumeId"] = "fsvol-" + randHex(17)
			d["OpenZFSConfiguration"] = z
		}
		s2Set(c, nil, d)
		return c.Fill(c.Res), nil
	})
	Override("fsx", "DeleteFileSystem", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		out["Lifecycle"] = "DELETING"
		return out, nil
	})
}
