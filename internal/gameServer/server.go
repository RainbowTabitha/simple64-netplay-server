package gameserver

import (
	"encoding/binary"
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
	IP      net.IP
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
	// Save state synchronization fields
	SaveStateData      map[uint32][]byte // Store save state data by ID
	SaveStateMutex     sync.Mutex
	SaveStateEnabled   bool
	SaveStateInterval  int
	MaxSaveStateSize   int
	// TCP connection tracking for broadcasting
	ActiveConnections  map[*net.TCPConn]bool
	ConnectionsMutex   sync.Mutex
	// Player connection tracking
	PlayerConnections  map[byte]*net.TCPConn // Map player number to TCP connection
}

func (g *GameServer) CreateNetworkServers(basePort int, maxGames int, roomName string, gameName string, emulatorName string, logger logr.Logger, saveStateEnabled bool, saveStateInterval int, maxSaveStateSize int) int {
	g.Logger = logger.WithValues("game", gameName, "room", roomName, "emulator", emulatorName)
	
	// Initialize save state fields
	g.SaveStateData = make(map[uint32][]byte)
	g.SaveStateEnabled = saveStateEnabled
	g.SaveStateInterval = saveStateInterval
	g.MaxSaveStateSize = maxSaveStateSize
	
	// Log save state configuration
	if g.SaveStateEnabled {
		g.Logger.Info("save state synchronization enabled", "interval", g.SaveStateInterval, "maxSize", g.MaxSaveStateSize)
	} else {
		g.Logger.Info("save state synchronization disabled")
	}
	
	// Initialize connection tracking
	g.ActiveConnections = make(map[*net.TCPConn]bool)
	g.PlayerConnections = make(map[byte]*net.TCPConn)
	
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
	
	// Clean up save state data
	g.SaveStateMutex.Lock()
	g.SaveStateData = make(map[uint32][]byte)
	g.SaveStateMutex.Unlock()
	
	// Clean up active connections
	g.ConnectionsMutex.Lock()
	g.ActiveConnections = make(map[*net.TCPConn]bool)
	g.PlayerConnections = make(map[byte]*net.TCPConn)
	g.ConnectionsMutex.Unlock()
	
	g.Logger.Info("cleaned up save state data and connections")
}

func (g *GameServer) isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (g *GameServer) ManageBuffer() {
	for {
		if !g.Running {
			g.Logger.Info("done managing buffers")
			return
		}

		// Find the largest buffer health
		var bufferHealth int32 = -1
		for i := range 4 {
			if g.GameData.BufferHealth[i] != -1 && g.GameData.CountLag[i] == 0 {
				if g.GameData.BufferHealth[i] > bufferHealth {
					bufferHealth = g.GameData.BufferHealth[i]
				}
			}
		}

		// Adjust the buffer size
		if bufferHealth != -1 {
			if bufferHealth > g.BufferTarget && g.GameData.BufferSize > 0 {
				g.GameData.BufferSize--
				g.Logger.Info("reduced buffer size", "bufferHealth", bufferHealth, "bufferSize", g.GameData.BufferSize)
			} else if bufferHealth < g.BufferTarget {
				g.GameData.BufferSize++
				g.Logger.Info("increased buffer size", "bufferHealth", bufferHealth, "bufferSize", g.GameData.BufferSize)
			}
		}

		time.Sleep(time.Second * 5)
	}
}

func (g *GameServer) ManagePlayers() {
	time.Sleep(time.Second * DisconnectTimeoutS)
	for {
		playersActive := false // used to check if anyone is still around
		var i byte

		g.GameDataMutex.Lock() // PlayerAlive and Status can be modified by processUDP in a different thread
		for i = range 4 {
			_, ok := g.Registrations[i]
			if ok {
				if g.GameData.PlayerAlive[i] {
					g.Logger.Info("player status", "player", i, "regID", g.Registrations[i].RegID, "bufferSize", g.GameData.BufferSize, "bufferHealth", g.GameData.BufferHealth[i], "countLag", g.GameData.CountLag[i], "address", g.GameData.PlayerAddresses[i])
					playersActive = true
				} else {
					g.Logger.Info("play disconnected UDP", "player", i, "regID", g.Registrations[i].RegID, "address", g.GameData.PlayerAddresses[i])
					g.GameData.Status |= (0x1 << (i + 1))

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
					g.GameData.BufferHealth[i] = -1
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
		time.Sleep(time.Second * DisconnectTimeoutS)
	}
}

// Save state synchronization functions
func (g *GameServer) storeSaveState(id uint32, data []byte) {
	if !g.SaveStateEnabled {
		return
	}
	
	g.SaveStateMutex.Lock()
	defer g.SaveStateMutex.Unlock()
	
	// Check size limit
	if len(data) > g.MaxSaveStateSize {
		g.Logger.Error(fmt.Errorf("save state too large"), "save state exceeds size limit", 
			"size", len(data), "limit", g.MaxSaveStateSize, "id", id)
		return
	}
	
	// Store the save state data
	g.SaveStateData[id] = make([]byte, len(data))
	copy(g.SaveStateData[id], data)
	
	// Keep only the most recent save state to save memory
	// Remove old save states (keep only the current one)
	for oldID := range g.SaveStateData {
		if oldID != id {
			delete(g.SaveStateData, oldID)
		}
	}
	
	g.Logger.Info("stored save state", "id", id, "size", len(data))
}

func (g *GameServer) getSaveState(id uint32) ([]byte, bool) {
	g.SaveStateMutex.Lock()
	defer g.SaveStateMutex.Unlock()
	
	data, exists := g.SaveStateData[id]
	if !exists {
		return nil, false
	}
	
	// Return a copy of the data
	result := make([]byte, len(data))
	copy(result, data)
	return result, true
}

func (g *GameServer) broadcastSaveState(id uint32, data []byte) {
	if !g.SaveStateEnabled {
		return
	}
	
	g.Logger.Info("broadcasting save state", "id", id, "size", len(data))
	
	// Create the packet: [packet_type][save_state_id][data]
	packet := make([]byte, 5+len(data))
	packet[0] = RequestSendSaveState
	binary.BigEndian.PutUint32(packet[1:5], id)
	copy(packet[5:], data)
	
	// Broadcast to all active TCP connections except the host (player 0)
	g.ConnectionsMutex.Lock()
	defer g.ConnectionsMutex.Unlock()
	
	broadcastCount := 0
	for conn := range g.ActiveConnections {
		// Skip the host player's connection
		if conn == g.PlayerConnections[0] {
			continue
		}
		
		_, err := conn.Write(packet)
		if err != nil {
			g.Logger.Error(err, "failed to broadcast save state to connection", "id", id, "address", conn.RemoteAddr().String())
			// Remove failed connection
			delete(g.ActiveConnections, conn)
		} else {
			broadcastCount++
		}
	}
	
	g.Logger.Info("broadcasted save state", "id", id, "size", len(data), "recipients", broadcastCount)
}

func (g *GameServer) sendSaveStateToClient(conn *net.TCPConn, id uint32) {
	if !g.SaveStateEnabled {
		return
	}
	
	data, exists := g.getSaveState(id)
	if !exists {
		g.Logger.Error(fmt.Errorf("save state not found"), "requested save state not available", "id", id)
		return
	}
	
	// Create the packet: [packet_type][save_state_id][data]
	packet := make([]byte, 5+len(data))
	packet[0] = RequestSendSaveState
	binary.BigEndian.PutUint32(packet[1:5], id)
	copy(packet[5:], data)
	
	_, err := conn.Write(packet)
	if err != nil {
		g.Logger.Error(err, "failed to send save state to client", "id", id, "address", conn.RemoteAddr().String())
	} else {
		g.Logger.Info("sent save state to client", "id", id, "size", len(data), "address", conn.RemoteAddr().String())
	}
}
