package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"aion-kernel/internal/orchestrator"
	"github.com/joho/godotenv"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	command := os.Args[1]
	if command == "--help" || command == "-h" || command == "help" {
		printUsage()
		os.Exit(0)
	}
	if command != "server" && command != "run" {
		fmt.Fprintf(os.Stderr, "aion-kernel: unknown command %q\n", command)
		printUsageAndExit()
	}

	// Setup flags for subcommands
	flagSet := flag.NewFlagSet(command, flag.ExitOnError)
	configPath := flagSet.String("config", "", "path to configuration file")
	workDir := flagSet.String("workdir", "", "project working directory (defaults to current directory)")
	projectRootFlag := flagSet.String("project-root", "", "alias for --workdir")

	var userPrompt string
	if command == "run" {
		flagSet.StringVar(&userPrompt, "prompt", "", "user task/prompt to execute")
	}

	flagSet.Parse(os.Args[2:])

	projectRoot, err := resolveProjectRoot(*workDir, *projectRootFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aion-kernel: invalid project root: %v\n", err)
		os.Exit(1)
	}

	loadEnvironment(projectRoot)

	config, configSource, err := orchestrator.LoadConfigWithFallback(*configPath, projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aion-kernel: failed to load config: %v\n", err)
		os.Exit(1)
	}
	log.Printf("aion-kernel: project root: %s", projectRoot)
	log.Printf("aion-kernel: config source: %s", configSource)

	daemon, err := orchestrator.NewDaemon(config, projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aion-kernel: failed to initialize: %v\n", err)
		os.Exit(1)
	}

	switch command {
	case "server":
		if err := daemon.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "aion-kernel: failed to start: %v\n", err)
			os.Exit(1)
		}
		log.Println("aion-kernel: server running, press Ctrl+C to stop")
		daemon.WaitForShutdown()

	case "run":
		if userPrompt == "" {
			fmt.Fprintln(os.Stderr, "aion-kernel run: missing --prompt flag")
			os.Exit(1)
		}

		if err := daemon.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "aion-kernel: failed to start: %v\n", err)
			os.Exit(1)
		}

		ctx := context.Background()
		if err := daemon.SubmitWork(ctx, userPrompt); err != nil {
			fmt.Fprintf(os.Stderr, "error submitting work: %v\n", err)
			daemon.Shutdown()
			os.Exit(1)
		}

		fmt.Println("aion-kernel: Work submitted. Monitoring execution...")

		monitorCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run convergence monitor
		go func() {
			err := daemon.MonitorExecution(monitorCtx)
			if err != nil {
				log.Printf("monitor error: %v", err)
				daemon.Shutdown()
				os.Exit(1)
			}

			if err := daemon.CloseProject(); err != nil {
				log.Printf("close project error: %v", err)
				os.Exit(1)
			}
			os.Exit(0)
		}()

		// Wait for manual termination or successful exit from monitor
		daemon.WaitForShutdown()

	default:
		fmt.Fprintf(os.Stderr, "aion-kernel: unknown command %q\n", command)
		printUsageAndExit()
	}
}

func resolveProjectRoot(workDir, projectRootFlag string) (string, error) {
	if workDir != "" && projectRootFlag != "" && workDir != projectRootFlag {
		return "", fmt.Errorf("--workdir and --project-root disagree")
	}
	root := workDir
	if root == "" {
		root = projectRootFlag
	}
	if root == "" {
		var err error
		root, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", abs)
	}
	return abs, nil
}

func loadEnvironment(projectRoot string) {
	globalEnv := filepath.Join(os.Getenv("HOME"), ".config", "aion-kernel", ".env")
	if home, err := os.UserHomeDir(); err == nil {
		globalEnv = filepath.Join(home, ".config", "aion-kernel", ".env")
	}

	if err := godotenv.Load(globalEnv); err == nil {
		log.Printf("aion-kernel: loaded global env defaults from %s", globalEnv)
	} else {
		log.Println("aion-kernel: notice: no global env defaults found")
	}

	projectEnv := filepath.Join(projectRoot, ".env")
	if err := godotenv.Overload(projectEnv); err == nil {
		log.Printf("aion-kernel: loaded project env overrides from %s", projectEnv)
	} else {
		log.Println("aion-kernel: notice: no project .env file found")
	}
}

func printUsageAndExit() {
	printUsage()
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: aion-kernel <command> [options]\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  server    Start orchestrator server daemon\n")
	fmt.Fprintf(os.Stderr, "  run       Execute a task across the project\n")
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	fmt.Fprintf(os.Stderr, "  --workdir <dir>       Project directory to operate on (default: current directory)\n")
	fmt.Fprintf(os.Stderr, "  --project-root <dir>  Alias for --workdir\n")
	fmt.Fprintf(os.Stderr, "  --config <file>       Config file path relative to project root, or absolute\n")
}
