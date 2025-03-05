package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-logr/zapr"
	gameserver "github.com/gopher64/gopher64-netplay-server/internal/gameServer"
	lobbyserver "github.com/gopher64/gopher64-netplay-server/internal/lobbyServer"
	"go.uber.org/zap"
)

const (
	DefaultBasePort    = 45000
	DefaultMOTDMessage = "Please consider <a href=\"https://www.patreon.com/loganmc10\">subscribing to the Patreon</a> or " +
		"<a href=\"https://github.com/sponsors/loganmc10\">supporting this project on GitHub.</a> Your support is needed in order to keep the netplay service online."
)

func newZap(logPath string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stderr"}
	if logPath != "" {
		cfg.OutputPaths = append(cfg.OutputPaths, logPath)
	}
	return cfg.Build() //nolint:wrapcheck
}

func main() {
	name := flag.String("name", "local-server", "Server name")
	basePort := flag.Int("baseport", DefaultBasePort, "Base port")
	disableBroadcast := flag.Bool("disable-broadcast", false, "Disable LAN broadcast")
	logPath := flag.String("log-path", "", "Write logs to this file")
	motd := flag.String("motd", "", "MOTD message to display to clients")
	maxGames := flag.Int("max-games", 10, "Maximum number of concurrent games") //nolint:gomnd,mnd
	enableAuth := flag.Bool("enable-auth", false, "Enable client authentication")
	enableRollback := flag.Bool("rollback", false, "Enable rollback netplay mode")
	rollbackPort := flag.Int("port", DefaultBasePort, "Port for rollback server (only used with -rollback)")
	flag.Parse()

	zapLog, err := newZap(*logPath)
	if err != nil {
		log.Panic(err)
	}
	logger := zapr.NewLogger(zapLog)

	// If rollback mode is enabled, start a standalone rollback server
	if *enableRollback {
		logger.Info("Starting standalone rollback server", "port", *rollbackPort)
		
		g := &gameserver.GameServer{
			Logger:           logger,
			Port:             *rollbackPort,
			GameName:         "Rollback Server",
			IsRollback:       true,
			RollbackDelay:    2, // Default to 2 frame delay
			MaxRollbackFrames: 7,
			Running:          true,
		}
		
		// Initialize the game server
		g.GameData.Status = 0
		g.Registrations = make(map[byte]*gameserver.Registration)
		g.TCPFiles = make(map[string][]byte)
		g.CustomData = make(map[byte][]byte)
		g.Features = make(map[string]string)
		g.Features[lobbyserver.FeatureRollback] = "1"
		g.StartTime = time.Now()
		
		// Create the network servers
		port := g.CreateNetworkServers(*basePort, *maxGames, "Rollback Server", "Rollback Mode", "simple64", logger)
		logger.Info("Rollback server started", "port", port)
		
		// Keep the server running
		for g.Running {
			time.Sleep(1 * time.Second)
		}
		
		return
	}

	// Standard lobby server mode
	if *name == "" {
		logger.Error(fmt.Errorf("name required"), "server name not set")
		os.Exit(1)
	}

	if *motd == "" {
		*motd = DefaultMOTDMessage
	}

	s := lobbyserver.LobbyServer{
		Logger:           logger,
		Name:             *name,
		BasePort:         *basePort,
		DisableBroadcast: *disableBroadcast,
		Motd:             *motd,
		MaxGames:         *maxGames,
		EnableAuth:       *enableAuth,
	}
	go s.LogServerStats()
	if err := s.RunSocketServer(DefaultBasePort); err != nil {
		logger.Error(err, "could not run socket server")
	}
}
