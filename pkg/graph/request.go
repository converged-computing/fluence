package graph

import "fmt"

type MatchAllocateRequest struct {
	Number     uint64
	Allocation string
	Spec       string
}

func (i *MatchAllocateRequest) Satisfied() bool {
	return i.Allocation != ""
}

// Show the customer their final request
func (i *MatchAllocateRequest) Show() {
	if i.Satisfied() {
		fmt.Printf("\n😋 Your resources are satisfied.\n")
	} else {
		fmt.Printf("\n😭️ We could not satisfy your request.\n")
	}
}
