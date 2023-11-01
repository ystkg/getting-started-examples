package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
)

func main() {
	lines, err := readLines("README.md")
	if err != nil {
		log.Fatal(err)
	}

	for i, v := range lines {
		fmt.Printf("%3d:%s\n", i+1, v)
	}
}

func readLines(name string) ([]string, error) {
	buf, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	lines := []string{}

	scanner := bufio.NewScanner(bytes.NewReader(buf))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, nil
}
