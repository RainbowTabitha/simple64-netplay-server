package gameserver

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/net/websocket"
)

type Client struct {
	Socket  *websocket.Conn
	IP      string
	Number  int
	InLobby bool
}

type Registration struct {
	RegID  uint32
	Plugin byte
	Raw    byte
}

type GameServer struct {
	StartTime          time.Time
	Players            map[string]Client
	PlayersMutex       sync.Mutex
	TCPListener        *net.TCPListener
	UDPListener        *net.UDPConn
	Registrations      map[byte]*Registration
	RegistrationsMutex sync.Mutex
	TCPMutex           sync.Mutex
	TCPFiles           map[string][]byte
	CustomData         map[byte][]byte
	Logger             logr.Logger
	GameName           string
	Password           string
	ClientSha          string
	MD5                string
	Emulator           string
	TCPSettings        []byte
	GameData           GameData
	GameDataMutex      sync.Mutex
	Port               int
	HasSettings        bool
	Running            bool
	Features           map[string]string
	NeedsUpdatePlayers bool
	NumberOfPlayers    int
	BufferTarget       int32
	IsRollback         bool
	RollbackDelay      int
	MaxRollbackFrames  int
	// Performance metrics
	PacketsProcessed   uint64
	PacketsDropped     uint64
	LastBufferAdapt    time.Time
}

const (
	BufferTarget int32 = 3
	RollbackBufferTarget int32 = 2
	MaxRollbackFrames = 7
	// Buffer management constants
	BufferAdaptIntervalStandard = 5 * time.Second
	BufferAdaptIntervalRollback = 2 * time.Second
	RollbackFixedBuffer    uint32 = 3  // Increased from 2 to 3 for better stability
	StandardMinBuffer     uint32 = 0
	StandardMaxBuffer     uint32 = 8
	// Performance monitoring constants
	PerformanceLogInterval = 10 * time.Second
	// Jitter reduction constants
	JitterThreshold        uint32 = 2  // Threshold for detecting jitter
	JitterRecoveryFrames   uint32 = 10 // Number of frames to wait before adjusting after jitter
)

func (g *GameServer) getBufferTarget() int32 {
	if g.IsRollback {
		return RollbackBufferTarget
	}
	return BufferTarget
}

func (g *GameServer) CreateNetworkServers(basePort int, maxGames int, roomName string, gameName string, emulatorName string, logger logr.Logger) int {
	g.Logger = logger.WithValues("game", gameName, "room", roomName, "emulator", emulatorName)
	port := g.createTCPServer(basePort, maxGames)
	if port == 0 {
		return port
	}
	if err := g.createUDPServer(); err != nil {
		g.Logger.Error(err, "error creating UDP server")
		if err := g.TCPListener.Close(); err != nil && !g.isConnClosed(err) {
			g.Logger.Error(err, "error closing TcpListener")
		}
		return 0
	}
	return port
}

func (g *GameServer) CloseServers() {
	if err := g.UDPListener.Close(); err != nil && !g.isConnClosed(err) {
		g.Logger.Error(err, "error closing UdpListener")
	} else if err == nil {
		g.Logger.Info("UDP server closed")
	}
	if err := g.TCPListener.Close(); err != nil && !g.isConnClosed(err) {
		g.Logger.Error(err, "error closing TcpListener")
	} else if err == nil {
		g.Logger.Info("TCP server closed")
	}
}

func (g *GameServer) isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (g *GameServer) ManageBuffer() {
	g.LastBufferAdapt = time.Now()
	lastPerformanceLog := time.Now()
	
	// Track jitter for better stability
	jitterDetected := false
	jitterRecoveryCountdown := uint32(0)
	
	for {
		if !g.Running {
			g.Logger.Info("done managing buffers")
			return
		}
		
		// Only perform dynamic buffer management for standard netplay
		// For rollback mode, we maintain fixed buffer sizes
		if !g.IsRollback {
			// Only adapt if enough time has passed since last adaptation
			if time.Since(g.LastBufferAdapt) >= BufferAdaptIntervalStandard {
				g.LastBufferAdapt = time.Now()
				
				// Adjust the buffer size for the lead player(s)
				for i := range 4 {
					if g.GameData.BufferHealth[i] != -1 && g.GameData.CountLag[i] == 0 {
						// Standard buffer management for regular netplay
						if g.GameData.BufferHealth[i] > g.getBufferTarget() && g.GameData.BufferSize[i] > StandardMinBuffer {
							g.GameData.BufferSize[i]--
							g.Logger.V(1).Info("reducing buffer size", "player", i, "bufferSize", g.GameData.BufferSize[i])
						} else if g.GameData.BufferHealth[i] < g.getBufferTarget() && g.GameData.BufferSize[i] < StandardMaxBuffer {
							g.GameData.BufferSize[i]++
							g.Logger.V(1).Info("increasing buffer size", "player", i, "bufferSize", g.GameData.BufferSize[i])
						}
					}
				}
			}
		} else {
			// For rollback mode, ensure buffer sizes remain fixed
			// This is more in line with traditional rollback netcode like GGPO
			
			// Check for jitter by monitoring lead count changes
			if jitterRecoveryCountdown > 0 {
				jitterRecoveryCountdown--
			} else {
				// Monitor for sudden large changes in lead count which indicate jitter
				for i := range 4 {
					if g.GameData.PlayerAlive[i] && g.GameData.BufferHealth[i] != -1 {
						// If we detect jitter, enter recovery mode
						if g.GameData.CountLag[i] > JitterThreshold && !jitterDetected {
							jitterDetected = true
							jitterRecoveryCountdown = JitterRecoveryFrames
							g.Logger.Info("jitter detected, entering recovery mode", 
								"player", i, 
								"lag", g.GameData.CountLag[i])
							
							// During jitter recovery, we'll be more conservative with buffer changes
							// and ensure all buffer sizes are at the fixed value
							for j := range 4 {
								if g.GameData.BufferSize[j] != RollbackFixedBuffer && g.GameData.BufferHealth[j] != -1 {
									g.GameData.BufferSize[j] = RollbackFixedBuffer
									g.Logger.V(1).Info("reset to fixed rollback buffer size during jitter recovery", 
										"player", j, 
										"bufferSize", g.GameData.BufferSize[j])
								}
							}
						}
					}
				}
				
				// Reset jitter detection after recovery period
				if jitterRecoveryCountdown == 0 {
					jitterDetected = false
				}
			}
			
			// Always ensure fixed buffer sizes in rollback mode
			for i := range 4 {
				if g.GameData.BufferSize[i] != RollbackFixedBuffer && g.GameData.BufferHealth[i] != -1 {
					g.GameData.BufferSize[i] = RollbackFixedBuffer
					g.Logger.V(1).Info("reset to fixed rollback buffer size", "player", i, "bufferSize", g.GameData.BufferSize[i])
				}
			}
		}
		
		// Log performance metrics periodically
		if time.Since(lastPerformanceLog) >= PerformanceLogInterval {
			lastPerformanceLog = time.Now()
			
			// Calculate packet loss percentage
			var packetLossPercent float64 = 0
			totalPackets := g.PacketsProcessed + g.PacketsDropped
			if totalPackets > 0 {
				packetLossPercent = float64(g.PacketsDropped) / float64(totalPackets) * 100
			}
			
			g.Logger.Info("netplay performance", 
				"rollbackMode", g.IsRollback,
				"packetsProcessed", g.PacketsProcessed,
				"packetsDropped", g.PacketsDropped,
				"packetLoss", fmt.Sprintf("%.2f%%", packetLossPercent),
				"uptime", time.Since(g.StartTime).String())
		}
		
		// Sleep for a shorter time to be more responsive but still avoid excessive CPU usage
		time.Sleep(50 * time.Millisecond) // Reduced from 100ms for more responsive jitter detection
	}
}

func (g *GameServer) ManagePlayers() {
	time.Sleep(time.Second * DisconnectTimeoutS)
	
	// Use a shorter timeout for rollback mode to detect disconnects faster
	disconnectTimeout := DisconnectTimeoutS
	if g.IsRollback {
		disconnectTimeout = DisconnectTimeoutS / 2
	}
	
	for {
		playersActive := false // used to check if anyone is still around
		var i byte

		g.GameDataMutex.Lock() // PlayerAlive and Status can be modified by processUDP in a different thread
		for i = range 4 {
			_, ok := g.Registrations[i]
			if ok {
				if g.GameData.PlayerAlive[i] {
					// In rollback mode, log more detailed metrics for debugging
					if g.IsRollback {
						g.Logger.Info("player status", 
							"player", i, 
							"regID", g.Registrations[i].RegID, 
							"bufferSize", g.GameData.BufferSize[i], 
							"bufferHealth", g.GameData.BufferHealth[i], 
							"countLag", g.GameData.CountLag[i], 
							"address", g.GameData.PlayerAddresses[i])
					} else {
						g.Logger.Info("player status", 
							"player", i, 
							"regID", g.Registrations[i].RegID, 
							"bufferSize", g.GameData.BufferSize[i], 
							"bufferHealth", g.GameData.BufferHealth[i], 
							"countLag", g.GameData.CountLag[i], 
							"address", g.GameData.PlayerAddresses[i])
					}
					playersActive = true
				} else {
					// Handle player disconnect
					g.Logger.Info("player disconnected UDP", "player", i, "regID", g.Registrations[i].RegID, "address", g.GameData.PlayerAddresses[i])
					g.GameData.Status |= (0x1 << (i + 1)) //nolint:gomnd,mnd

					g.RegistrationsMutex.Lock() // Registrations can be modified by processTCP
					delete(g.Registrations, i)
					g.RegistrationsMutex.Unlock()

					for k, v := range g.Players {
						if v.Number == int(i) {
							g.PlayersMutex.Lock()
							delete(g.Players, k)
							g.NeedsUpdatePlayers = true
							g.PlayersMutex.Unlock()
						}
					}
				}
			}
			g.GameData.PlayerAlive[i] = false
		}
		g.GameDataMutex.Unlock()

		if !playersActive {
			g.Logger.Info("no more players, closing room", "numPlayers", g.NumberOfPlayers, "playTime", time.Since(g.StartTime).String())
			g.CloseServers()
			g.Running = false
			return
		}
		time.Sleep(time.Second * time.Duration(disconnectTimeout))
	}
}
