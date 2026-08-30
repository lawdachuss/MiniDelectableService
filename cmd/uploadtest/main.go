package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/teacat/chaturbate-dvr/uploader"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: uploadtest <image_path>")
		fmt.Println("Example: uploadtest /tmp/test_upload.png")
		os.Exit(1)
	}

	// Load .env file
	loadDotEnv(".env")

	filePath := os.Args[1]
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Fatalf("File not found: %s", filePath)
	}

	fmt.Printf("Testing upload chain with: %s\n\n", filePath)

	// Create the multi-image uploader
	imgUploader := uploader.NewMultiImageUploader()

	// Test UploadToAll (parallel upload to all hosts)
	fmt.Println("=== Testing UploadToAll (parallel) ===")
	results := imgUploader.UploadToAll(filePath, nil)

	fmt.Printf("\nResults (%d hosts):\n", len(results))
	for _, r := range results {
		if r.Err != nil {
			fmt.Printf("  ❌ %-15s Error: %v\n", r.Host, r.Err)
		} else {
			fmt.Printf("  ✅ %-15s URL: %s\n", r.Host, r.URL)
		}
	}

	// Count successes
	successCount := 0
	for _, r := range results {
		if r.Err == nil && r.URL != "" {
			successCount++
		}
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total hosts: %d\n", len(results))
	fmt.Printf("Succeeded:   %d\n", successCount)
	fmt.Printf("Failed:      %d\n", len(results)-successCount)

	if successCount == 0 {
		fmt.Println("\n❌ All hosts failed!")
		os.Exit(1)
	}

	fmt.Printf("\n✅ %d/%d hosts uploaded successfully!\n", successCount, len(results))
}

func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
