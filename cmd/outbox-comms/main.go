// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command outbox-comms runs the notification service and its operator commands.
package main

import (
	"context"
	"os"

	"github.com/tetriz-ai/outbox-comms/cli"
)

func main() {
	os.Exit(cli.Main(context.Background(), os.Args[1:]))
}
