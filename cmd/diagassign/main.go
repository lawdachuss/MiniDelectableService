package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/teacat/chaturbate-dvr/database"
)

func main() {
	url := os.Getenv("SUPABASE_URL")
	key := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	if url == "" {
		url = os.Getenv("SUPABASE_URL_FALLBACK")
	}
	if url == "" || key == "" {
		fmt.Println("need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
		os.Exit(1)
	}
	c := database.NewClient(url, key)

	nodes, err := c.GetNodes()
	if err != nil {
		fmt.Printf("GetNodes ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("GetNodes OK: %d nodes\n", len(nodes))

	all, err := c.GetAllAssignments()
	if err != nil {
		fmt.Printf("GetAllAssignments ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("GetAllAssignments OK: %d rows\n", len(all))

	un := 0
	for _, a := range all {
		if a.AssignedNode == "" {
			un++
		}
	}
	fmt.Printf("unassigned in fetched set: %d\n", un)

	sort.Slice(all, func(i, j int) bool { return all[i].Username < all[j].Username })
	tried := 0
	for _, a := range all {
		if a.AssignedNode != "" {
			continue
		}
		// claim to node-1 like a Step-A would
		ok, err := c.ClaimSpecificChannel(a.Username, a.Site, "node-1")
		fmt.Printf("claim %s/%s -> node-1: ok=%v err=%v\n", a.Site, a.Username, ok, err)
		tried++
		if tried >= 5 {
			break
		}
	}
}
