package gameserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"sync"
	"time"
	
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// Constants for buffer sizes
const (
	// Packet types
	KeyInfoClient           = 0
	KeyInfoServer           = 1
	PlayerInputRequest      = 2
	KeyInfoServerGratuitous = 3
	CP0Info                 = 4

	// Status and configuration constants
	StatusDesync              = 1
	DisconnectTimeoutS        = 30
	NoRegID                   = 255
	InputDataMax       uint32 = 5000
	CS4                       = 64
	
	// Buffer sizes
	defaultUDPReadBuffer  = 1500
	defaultUDPWriteBuffer = 508
	defaultSyncValueSize  = 128 // Size of sync values in CP0Info packets (133-5)
)

// GameData holds all the game state information
type GameData struct {
	sync.RWMutex
	SyncValues      map[uint32][]byte
	PlayerAddresses []*net.UDPAddr
	BufferSize      []uint32
	BufferHealth    []int32
	Inputs          []map[uint32]uint32
	Plugin          []map[uint32]byte
	PendingInput    []uint32
	CountLag        []uint32
	PendingPlugin   []byte
	PlayerAlive     []bool
	LeadCount       uint32
	Status          byte
	LobbyBufferSize int
}

// Add these fields to your existing GameServer struct
type bufferPoolManager struct {
	pool sync.Pool
	ctx  context.Context
	wg   sync.WaitGroup
}

// Initialize buffer pool manager for the GameServer
func (g *GameServer) initBufferPool() {
	ctx, cancel := context.WithCancel(context.Background())
	g.bufferPoolMgr = &bufferPoolManager{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, defaultUDPReadBuffer)
			},
		},
		ctx: ctx,
	}
	g.cancelFunc = cancel
}

// Shutdown cleans up resources
func (g *GameServer) Shutdown() {
	if g.cancelFunc != nil {
		g.cancelFunc()
	}
	
	if g.UDPListener != nil {
		_ = g.UDPListener.Close()
	}
	
	if g.bufferPoolMgr != nil {
		g.bufferPoolMgr.wg.Wait()
	}
}

// fillInput ensures input data exists for a given player and count
func (g *GameServer) fillInput(playerNumber byte, count uint32) {
	// Use a mutex for thread safety (you might need to add this to GameData)
	g.GameDataMutex.Lock()
	defer g.GameDataMutex.Unlock()
	
	_, inputExists := g.GameData.Inputs[playerNumber][count]
	
	// Only delete if old input exists
	oldCount := count - InputDataMax
	if _, exists := g.GameData.Inputs[playerNumber][oldCount]; exists {
		delete(g.GameData.Inputs[playerNumber], oldCount)
	}
	
	if _, exists := g.GameData.Plugin[playerNumber][oldCount]; exists {
		delete(g.GameData.Plugin[playerNumber], oldCount)
	}
	
	if !inputExists {
		g.GameData.Inputs[playerNumber][count] = g.GameData.PendingInput[playerNumber]
		g.GameData.Plugin[playerNumber][count] = g.GameData.PendingPlugin[playerNumber]
	}
}

// adjustBuffers optimizes buffer sizes based on player lag
func (g *GameServer) adjustBuffers() uint32 {
	g.GameDataMutex.Lock()
	// Make local copies of needed data to minimize lock duration
	countLags := make([]uint32, len(g.GameData.CountLag))
	bufferSizes := make([]uint32, len(g.GameData.BufferSize))
	leadCount := g.GameData.LeadCount
	lobbyBufferSize := g.GameData.LobbyBufferSize
	copy(countLags, g.GameData.CountLag)
	copy(bufferSizes, g.GameData.BufferSize)
	g.GameDataMutex.Unlock()

	maxLag := uint32(0)
	firstLag := countLags[0]
	sameLag := true
	allPlayersBelowThreshold := true

	// First pass: collect information about lags
	for i, lag := range countLags {
		// Track maximum lag
		if lag > maxLag {
			maxLag = lag
		}
		
		// Check if all lags are the same
		if lag != firstLag {
			sameLag = false
		}
		
		// Check if any player has lag exceeding threshold
		threshold := uint32(20)
		if lag > threshold {
			allPlayersBelowThreshold = false
		}
	}

	// Early return if no adjustment needed
	if sameLag || allPlayersBelowThreshold {
		return 0
	}

	// Adjust buffer sizes based on lag conditions
	for i := 0; i < len(bufferSizes); i++ {
		countLag := countLags[i]
	
		// Log if countLag exceeds LeadCount
		if countLag > leadCount {
			g.Logger.Error(fmt.Errorf("bad count lag"), "count is larger than LeadCount",
				"count", countLag, "LeadCount", leadCount, "playerNumber", i)
		}
	
		if !allPlayersBelowThreshold {
			if countLag == 0 {
				// If the lag is 0, set buffer to 1
				g.updateBufferSize(2, i)
				
				// Use a background goroutine with cleanup capability
				if g.bufferPoolMgr != nil {
					bufferCtx, bufferCancel := context.WithCancel(g.bufferPoolMgr.ctx)
					g.bufferPoolMgr.wg.Add(1)
					
					go func(ctx context.Context, index int, duration int, cancel context.CancelFunc) {
						defer g.bufferPoolMgr.wg.Done()
						defer cancel()
						
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Duration(duration) * time.Second):
							g.updateBufferSize(int32(duration), index)
						}
					}(bufferCtx, i, lobbyBufferSize, bufferCancel)
				} else {
					// Fallback to original implementation if buffer pool manager not initialized
					go func(index int) {
						time.Sleep(time.Duration(lobbyBufferSize) * time.Second)
						g.updateBufferSize(int32(lobbyBufferSize), i)
					}(i)
				}
			} else {
				// Reset buffer size to the original value from LobbyBufferSize
				g.updateBufferSize(int32(lobbyBufferSize), i)
			}
		} else {
			// Reset buffer size to the original value from LobbyBufferSize
			g.updateBufferSize(int32(lobbyBufferSize), i)
		}
	}
	
	return maxLag
}

func uintLarger(a, b uint32) bool {
    return a > b
}

// sendUDPInput sends input data to players via UDP with optimized buffer usage
func (g *GameServer) sendUDPInput(count uint32, addr *net.UDPAddr, playerNumber byte, spectator bool, sendingPlayerNumber byte) uint32 {
	var buffer []byte
	
	// Use buffer pool if available
	if g.bufferPoolMgr != nil {
		bufferInt := g.bufferPoolMgr.pool.Get()
		defer g.bufferPoolMgr.pool.Put(bufferInt)
		buffer = bufferInt.([]byte)[:defaultUDPWriteBuffer]
	} else {
		// Fallback to original implementation
		buffer = make([]byte, defaultUDPWriteBuffer)
	}

	var countLag uint32
	g.GameDataMutex.Lock()
	leadCount := g.GameData.LeadCount
	g.GameDataMutex.Unlock()
	
	if uintLarger(count, leadCount) {
		if !spectator {
			g.Logger.Error(fmt.Errorf("bad count lag"), "count is larger than LeadCount", 
				"count", count, "LeadCount", leadCount, "playerNumber", playerNumber)
		}
	} else {
		countLag = leadCount - count
	}

	g.adjustBuffers()

	// Prepare packet header
	if sendingPlayerNumber == NoRegID {
		sendingPlayerNumber = playerNumber
		buffer[0] = KeyInfoServerGratuitous
	} else {
		buffer[0] = KeyInfoServer
	}
	
	buffer[1] = playerNumber
	
	g.GameDataMutex.Lock()
	buffer[2] = g.GameData.Status
	bufferSize := g.GameData.BufferSize[sendingPlayerNumber]
	g.GameDataMutex.Unlock()
	
	buffer[3] = uint8(countLag)
	currentByte := 5
	
	start := count
	end := start + bufferSize
	
	// Check if input exists for this count
	g.GameDataMutex.Lock()
	_, ok := g.GameData.Inputs[playerNumber][count]
	g.GameDataMutex.Unlock()
	
	// Build packet body with inputs
	for (currentByte < len(buffer)-9) && ((!spectator && countLag == 0 && uintLarger(end, count)) || ok) {
		binary.BigEndian.PutUint32(buffer[currentByte:], count)
		currentByte += 4
		
		g.fillInput(playerNumber, count)
		
		g.GameDataMutex.Lock()
		inputValue := g.GameData.Inputs[playerNumber][count]
		pluginValue := g.GameData.Plugin[playerNumber][count]
		g.GameDataMutex.Unlock()
		
		binary.BigEndian.PutUint32(buffer[currentByte:], inputValue)
		currentByte += 4
		buffer[currentByte] = pluginValue
		currentByte++
		count++
		
		g.GameDataMutex.Lock()
		_, ok = g.GameData.Inputs[playerNumber][count]
		g.GameDataMutex.Unlock()
	}

	// Send packet if it contains data
	if count > start {
		buffer[4] = uint8(count - start) // number of counts in packet
		_, err := g.UDPListener.WriteToUDP(buffer[0:currentByte], addr)
		if err != nil {
			g.Logger.Error(err, "could not send input")
		}
	}
	
	return countLag
}

// processUDP handles incoming UDP packets with optimized concurrency
func (g *GameServer) processUDP(addr *net.UDPAddr, buf []byte) {
	playerNumber := buf[1]
	
	switch buf[0] {
	case KeyInfoClient:
		g.GameDataMutex.Lock()
		g.GameData.PlayerAddresses[playerNumber] = addr
		g.GameData.PendingInput[playerNumber] = binary.BigEndian.Uint32(buf[6:])
		g.GameData.PendingPlugin[playerNumber] = buf[10]
		
		// Make a local copy of player addresses while under lock
		playerAddresses := make([]*net.UDPAddr, len(g.GameData.PlayerAddresses)) 
		copy(playerAddresses, g.GameData.PlayerAddresses)
		g.GameDataMutex.Unlock()
		
		count := binary.BigEndian.Uint32(buf[2:])
		
		// Send update to all connected players
		for _, playerAddr := range playerAddresses {
			if playerAddr != nil {
				g.sendUDPInput(count, playerAddr, playerNumber, true, NoRegID)
			}
		}
		
	case PlayerInputRequest:
		regID := binary.BigEndian.Uint32(buf[2:])
		count := binary.BigEndian.Uint32(buf[6:])
		spectator := buf[10] != 0
		
		// Update lead count if needed
		if !spectator && uintLarger(count, g.GameData.LeadCount) {
			g.GameDataMutex.Lock()
			g.GameData.LeadCount = count
			g.GameDataMutex.Unlock()
		}
		
		sendingPlayerNumber, err := g.getPlayerNumberByID(regID)
		if err != nil {
			g.Logger.Error(err, "could not process request", "regID", regID)
			return
		}
		
		countLag := g.sendUDPInput(count, addr, playerNumber, spectator, sendingPlayerNumber)
		
		g.GameDataMutex.Lock()
		g.GameData.BufferHealth[sendingPlayerNumber] = int32(buf[11])
		g.GameData.PlayerAlive[sendingPlayerNumber] = true
		g.GameData.CountLag[sendingPlayerNumber] = countLag
		g.GameDataMutex.Unlock()
		
	case CP0Info:
		g.GameDataMutex.Lock()
		currentStatus := g.GameData.Status
		
		if currentStatus&StatusDesync == 0 {
			viCount := binary.BigEndian.Uint32(buf[1:])
			syncValue, ok := g.GameData.SyncValues[viCount]
			
			if !ok {
				// Store new sync value (make a copy to avoid reference issues)
				g.GameData.SyncValues[viCount] = append([]byte(nil), buf[5:133]...)
				g.GameDataMutex.Unlock()
			} else if !bytes.Equal(syncValue, buf[5:133]) {
				// Detect desync
				g.GameData.Status |= StatusDesync
				
				// Cache values for logging outside the lock
				numPlayers := g.NumberOfPlayers
				clientSha := g.ClientSha
				startTime := g.StartTime
				features := g.Features
				
				g.GameDataMutex.Unlock()
				
				g.Logger.Error(fmt.Errorf("desync"), "game has desynced", 
					"numPlayers", numPlayers, 
					"clientSHA", clientSha, 
					"playTime", time.Since(startTime).String(), 
					"features", features)
			} else {
				g.GameDataMutex.Unlock()
			}
		} else {
			g.GameDataMutex.Unlock()
		}
	}
}

// watchUDP optimized for memory usage and with proper shutdown handling
func (g *GameServer) watchUDP() {
	if g.bufferPoolMgr != nil {
		g.bufferPoolMgr.wg.Add(1)
		defer g.bufferPoolMgr.wg.Done()
	}
	
	for {
		var buf []byte
		var bufferInt interface{}
		
		// Use buffer pool if available
		if g.bufferPoolMgr != nil {
			// Check for shutdown signal
			select {
			case <-g.bufferPoolMgr.ctx.Done():
				return
			default:
				// Continue processing
			}
			
			bufferInt = g.bufferPoolMgr.pool.Get()
			buf = bufferInt.([]byte)
			
			// Set read deadline to allow context cancellation checks
			_ = g.UDPListener.SetReadDeadline(time.Now().Add(time.Second))
		} else {
			buf = make([]byte, 1500)
		}
		
		_, addr, err := g.UDPListener.ReadFromUDP(buf)
		if err != nil {
			// Return buffer to pool if we used one
			if bufferInt != nil {
				g.bufferPoolMgr.pool.Put(bufferInt)
			}
			
			if g.isConnClosed(err) {
				return
			}
			
			// Skip timeout errors which we use to check context
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				continue
			}
			
			g.Logger.Error(err, "error from UdpListener")
			continue
		}
		
		// Validate sender IP
		validated := false
		for _, v := range g.Players {
			if addr.IP.Equal(net.ParseIP(v.IP)) {
				validated = true
				break
			}
		}
		
		if !validated {
			g.Logger.Error(fmt.Errorf("invalid udp connection"), "bad IP", "IP", addr.IP)
			
			// Return buffer to pool if we used one
			if bufferInt != nil {
				g.bufferPoolMgr.pool.Put(bufferInt)
			}
			
			continue
		}
		
		// Process the valid packet
		// Make a copy of the data to avoid race conditions with the buffer pool
		dataCopy := make([]byte, len(buf))
		copy(dataCopy, buf)
		
		// Return buffer to pool if we used one
		if bufferInt != nil {
			g.bufferPoolMgr.pool.Put(bufferInt)
		}
		
		g.processUDP(addr, dataCopy)
	}
}

// updateBufferSize safely updates a player's buffer size
func (g *GameServer) updateBufferSize(newBuffer int32, playerIndex int) {
	g.GameDataMutex.Lock()
	defer g.GameDataMutex.Unlock()
	
	if playerIndex >= 0 && playerIndex < len(g.GameData.BufferSize) {
		g.GameData.BufferSize[playerIndex] = uint32(newBuffer)
	}
}

// createUDPServer initializes the UDP server with optimizations
func (g *GameServer) createUDPServer() error {
	var err error
	g.UDPListener, err = net.ListenUDP("udp", &net.UDPAddr{Port: g.Port})
	if err != nil {
		return err
	}
	
	// Set DSCP values for QoS
	if err := ipv4.NewConn(g.UDPListener).SetTOS(CS4 << 2); err != nil {
		g.Logger.Error(err, "could not set IPv4 DSCP")
	}
	if err := ipv6.NewConn(g.UDPListener).SetTrafficClass(CS4 << 2); err != nil {
		g.Logger.Error(err, "could not set IPv6 DSCP")
	}
	
	g.Logger.Info("Created UDP server", "port", g.Port)

	// Initialize game data
	g.GameData.PlayerAddresses = make([]*net.UDPAddr, 4)
	g.GameData.BufferSize = []uint32{5, 5, 5, 5}
	g.GameData.LobbyBufferSize = 5
	g.GameData.BufferHealth = []int32{-1, -1, -1, -1}
	g.GameData.Inputs = make([]map[uint32]uint32, 4)
	for i := range 4 {
		g.GameData.Inputs[i] = make(map[uint32]uint32)
	}
	g.GameData.Plugin = make([]map[uint32]byte, 4)
	for i := range 4 {
		g.GameData.Plugin[i] = make(map[uint32]byte)
	}
	g.GameData.PendingInput = make([]uint32, 4)
	g.GameData.PendingPlugin = make([]byte, 4)
	g.GameData.SyncValues = make(map[uint32][]byte)
	g.GameData.PlayerAlive = make([]bool, 4)
	g.GameData.CountLag = make([]uint32, 4)

	// Initialize buffer pooling for better memory management
	g.initBufferPool()

	go g.watchUDP()
	return nil
}