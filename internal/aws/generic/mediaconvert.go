package generic

// MediaConvert: a queue's ARN is queues/<name>. No job runs.
func init() {
	arnTemplates["mediaconvert/Queue"] = "queues/{Name}"
}
