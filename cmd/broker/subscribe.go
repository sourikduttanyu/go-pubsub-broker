package main

var (
	subTopic    string
	subID       string
	subFailRate float64
)

func init() {
	subscribeCmd.Flags().StringVarP(&subTopic, "topic", "t", "", "topic name (required)")
	subscribeCmd.Flags().StringVarP(&subID, "sub", "s", "", "subscription ID (required)")
	subscribeCmd.Flags().Float64VarP(&subFailRate, "fail-rate", "f", 0.0, "probability [0,1] that a delivery is nack'd (for demos)")
	_ = subscribeCmd.MarkFlagRequired("topic")
	_ = subscribeCmd.MarkFlagRequired("sub")
}
