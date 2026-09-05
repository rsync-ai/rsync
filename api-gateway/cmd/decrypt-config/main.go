package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rsync-ai/shared/crypto"
)

// Usage:
//   ENCRYPTION_KEY=... go run ./cmd/decrypt-config < encrypted.txt
// or
//   ENCRYPTION_KEY=... go run ./cmd/decrypt-config "BASE64_CIPHERTEXT"
func main() {
	var ciphertext string
	if len(os.Args) > 1 {
		ciphertext = os.Args[1]
	} else {
		// Read single line from stdin
		in := bufio.NewReader(os.Stdin)
		b, _ := in.ReadString('\n')
		ciphertext = b
	}
	ciphertext = strings.TrimSpace(ciphertext)
	if ciphertext == "" {
		fmt.Fprintln(os.Stderr, "missing ciphertext (pass as arg or stdin)")
		os.Exit(2)
	}

	plain, err := crypto.DecryptString(ciphertext)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decrypt failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(plain)
}

