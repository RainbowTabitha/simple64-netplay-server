package gameserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type GameData struct {
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
}

const (
	KeyInfoClient           = 0
	KeyInfoServer           = 1
	PlayerInputRequest      = 2
	KeyInfoServerGratuitous = 3
	CP0Info                 = 4
)

const (
	StatusDesync              = 1
	DisconnectTimeoutS        = 30
	NoRegID                   = 255
	InputDataMax       uint32 = 5000
	CS4                       = 32
)

// returns true if v is bigger than w (accounting for uint32 wrap around).
func uintLarger(v uint32, w uint32) bool {
	return (w - v) > (math.MaxUint32 / 2) //nolint:gomnd,mnd
}

func (g *GameServer) getPlayerNumberByID(regID uint32) (byte, error) {
	var i byte
	for i = range 4 {
		v, ok := g.Registrations[i]
		if ok {
			if v.RegID == regID {
				return i, nil
			}
		}
	}
	return NoRegID, fmt.Errorf("could not find ID")
}

func (g *GameServer) fillInput(playerNumber byte, count uint32) {
	_, inputExists := g.GameData.Inputs[playerNumber][count]
	
	// More aggressive garbage collection for old input data
	// Remove inputs that are very old (well beyond the rollback window)
	if count > InputDataMax*2 {
		for k := range g.GameData.Inputs[playerNumber] {
			if k < count-InputDataMax {
				delete(g.GameData.Inputs[playerNumber], k)
				delete(g.GameData.Plugin[playerNumber], k)
			}
		}
	} else {
		// More conservative approach if we're early in the game
		delete(g.GameData.Inputs[playerNumber], count-InputDataMax)
		delete(g.GameData.Plugin[playerNumber], count-InputDataMax)
	}
	
	if !inputExists {
		g.GameData.Inputs[playerNumber][count] = g.GameData.PendingInput[playerNumber]
		g.GameData.Plugin[playerNumber][count] = g.GameData.PendingPlugin[playerNumber]
	}
}

func (g *GameServer) sendUDPInput(count uint32, addr *net.UDPAddr, playerNumber byte, spectator bool, sendingPlayerNumber byte) uint32 {
	buffer := make([]byte, 508) //nolint:gomnd,mnd
	var countLag uint32
	
	// Calculate lag differently based on mode
	if g.IsRollback {
		// In rollback mode, we want to minimize reported lag to clients
		// This allows the client's rollback system to handle timing
		countLag = 0
	} else if uintLarger(count, g.GameData.LeadCount) {
		if !spectator {
			g.Logger.Error(fmt.Errorf("bad count lag"), "count is larger than LeadCount", "count", count, "LeadCount", g.GameData.LeadCount, "playerNumber", playerNumber)
		}
	} else {
		countLag = g.GameData.LeadCount - count
	}
	
	if sendingPlayerNumber == NoRegID { // if the incoming packet was KeyInfoClient, the regID isn't included in the packet
		sendingPlayerNumber = playerNumber
		buffer[0] = KeyInfoServerGratuitous // client will ignore countLag value in this case
	} else {
		buffer[0] = KeyInfoServer
	}
	buffer[1] = playerNumber
	buffer[2] = g.GameData.Status
	buffer[3] = uint8(countLag) //nolint:gosec
	currentByte := 5
	start := count
	
	// In rollback mode, we always use the fixed buffer size
	// In standard mode, we use the dynamic buffer size
	var end uint32
	if g.IsRollback {
		// For rollback, we want to send more frames ahead to improve prediction
		// This helps reduce the need for rollbacks and makes gameplay smoother
		end = start + RollbackFixedBuffer + 1 // Send one extra frame for better prediction
	} else {
		end = start + g.GameData.BufferSize[sendingPlayerNumber]
	}
	
	_, ok := g.GameData.Inputs[playerNumber][count] // check if input exists for this count
	for (currentByte < len(buffer)-9) && ((!spectator && countLag == 0 && uintLarger(end, count)) || ok) {
		binary.BigEndian.PutUint32(buffer[currentByte:], count)
		currentByte += 4
		g.fillInput(playerNumber, count)
		binary.BigEndian.PutUint32(buffer[currentByte:], g.GameData.Inputs[playerNumber][count])
		currentByte += 4
		buffer[currentByte] = g.GameData.Plugin[playerNumber][count]
		currentByte++
		count++
		_, ok = g.GameData.Inputs[playerNumber][count] // check if input exists for this count
	}

	if count > start {
		buffer[4] = uint8(count - start) //nolint:gosec // number of counts in packet
		
		// In rollback mode, set higher priority for packets
		if g.IsRollback {
			// Set DSCP to CS4 (32) for even higher priority than AF31
			// This ensures these packets get processed ahead of other traffic
			if err := ipv4.NewConn(g.UDPListener).SetTOS(CS4 << 2); err != nil {
				g.Logger.V(1).Info("could not set higher priority for input packet")
			}
		}
		
		_, err := g.UDPListener.WriteToUDP(buffer[0:currentByte], addr)
		if err != nil {
			g.Logger.Error(err, "could not send input")
			atomic.AddUint64(&g.PacketsDropped, 1)
		}
		
		// Reset DSCP back to AF31 after sending high-priority packet
		if g.IsRollback {
			if err := ipv4.NewConn(g.UDPListener).SetTOS(0x68); err != nil {
				g.Logger.V(1).Info("could not reset packet priority")
			}
		}
	}
	return countLag
}

func (g *GameServer) processUDP(addr *net.UDPAddr, buf []byte) {
	// Increment our packet counter for metrics
	atomic.AddUint64(&g.PacketsProcessed, 1)
	
	playerNumber := buf[1]
	if buf[0] == KeyInfoClient {
		g.GameData.PlayerAddresses[playerNumber] = addr
		count := binary.BigEndian.Uint32(buf[2:])

		g.GameData.PendingInput[playerNumber] = binary.BigEndian.Uint32(buf[6:])
		g.GameData.PendingPlugin[playerNumber] = buf[10]

		// In rollback mode, we want to process and distribute inputs as quickly as possible
		// This helps reduce perceived latency and improves prediction accuracy
		if g.IsRollback {
			// Immediately fill the input at the current count to ensure it's available
			g.fillInput(playerNumber, count)
			
			// High priority distribution to all players with minimal delay
			for i := range 4 {
				if g.GameData.PlayerAddresses[i] != nil {
					g.sendUDPInput(count, g.GameData.PlayerAddresses[i], playerNumber, false, NoRegID)
				}
			}
		} else {
			// Standard distribution for regular netplay
			for i := range 4 {
				if g.GameData.PlayerAddresses[i] != nil {
					g.sendUDPInput(count, g.GameData.PlayerAddresses[i], playerNumber, true, NoRegID)
				}
			}
		}
	} else if buf[0] == PlayerInputRequest {
		regID := binary.BigEndian.Uint32(buf[2:])
		count := binary.BigEndian.Uint32(buf[6:])
		spectator := buf[10]
		
		// Update lead count for non-spectators
		if uintLarger(count, g.GameData.LeadCount) && spectator == 0 {
			// In rollback mode, we want to be more conservative with lead count updates
			// to prevent jitter caused by frequent changes
			if g.IsRollback {
				// Only update if significantly ahead (more than 1 frame)
				if count > g.GameData.LeadCount+1 {
					g.GameData.LeadCount = count
				}
			} else {
				g.GameData.LeadCount = count
			}
		}
		
		sendingPlayerNumber, err := g.getPlayerNumberByID(regID)
		if err != nil {
			g.Logger.Error(err, "could not process request", "regID", regID)
			atomic.AddUint64(&g.PacketsDropped, 1)
			return
		}
		
		countLag := g.sendUDPInput(count, addr, playerNumber, spectator != 0, sendingPlayerNumber)
		
		// In rollback mode, we maintain a fixed buffer health value
		// This is more in line with traditional rollback netcode
		if g.IsRollback {
			// Always maintain the target buffer health for stability
			g.GameData.BufferHealth[sendingPlayerNumber] = RollbackBufferTarget
		} else {
			// In standard mode, use client-reported buffer health
			g.GameData.BufferHealth[sendingPlayerNumber] = int32(buf[11])
		}

		g.GameDataMutex.Lock() // PlayerAlive can be modified by ManagePlayers in a different thread
		g.GameData.PlayerAlive[sendingPlayerNumber] = true
		g.GameDataMutex.Unlock()

		// In rollback mode, we don't want to report lag to clients
		// This prevents the client from trying to adjust its own timing
		if g.IsRollback {
			g.GameData.CountLag[sendingPlayerNumber] = 0
		} else {
			g.GameData.CountLag[sendingPlayerNumber] = countLag
		}
	} else if buf[0] == CP0Info {
		if g.GameData.Status&StatusDesync == 0 {
			viCount := binary.BigEndian.Uint32(buf[1:])
			syncValue, ok := g.GameData.SyncValues[viCount]
			if !ok {
				g.GameData.SyncValues[viCount] = buf[5:133]
			} else if !bytes.Equal(syncValue, buf[5:133]) {
				g.GameDataMutex.Lock() // Status can be modified by ManagePlayers in a different thread
				g.GameData.Status |= StatusDesync
				g.GameDataMutex.Unlock()

				g.Logger.Error(fmt.Errorf("desync"), "game has desynced", "numPlayers", g.NumberOfPlayers, "clientSHA", g.ClientSha, "playTime", time.Since(g.StartTime).String(), "features", g.Features)
			}
		}
	} else {
		// Unknown packet type - count as dropped
		atomic.AddUint64(&g.PacketsDropped, 1)
	}
}

func (g *GameServer) watchUDP() {
	// Preallocate a larger buffer pool to handle more concurrent packets
	bufferPool := make([][]byte, 20) // Increased from 10
	for i := range bufferPool {
		bufferPool[i] = make([]byte, 1500) //nolint:gomnd,mnd
	}
	bufferIndex := 0

	// Create a channel for processing packets to better manage concurrency
	packetChan := make(chan struct {
		addr *net.UDPAddr
		data []byte
	}, 100) // Buffer size of 100 packets
	
	// Start multiple worker goroutines to process packets concurrently
	// This improves throughput and reduces processing latency
	if g.IsRollback {
		for i := 0; i < 4; i++ { // 4 worker goroutines for processing
			go func() {
				for packet := range packetChan {
					// Create a copy of the buffer for processing
					bufCopy := make([]byte, len(packet.data))
					copy(bufCopy, packet.data)
					g.processUDP(packet.addr, bufCopy)
				}
			}()
		}
	}

	for {
		// Use buffer from pool instead of allocating a new one each time
		buf := bufferPool[bufferIndex]
		bufferIndex = (bufferIndex + 1) % len(bufferPool)
		
		n, addr, err := g.UDPListener.ReadFromUDP(buf)
		if err != nil && !g.isConnClosed(err) {
			g.Logger.Error(err, "error from UdpListener")
			continue
		} else if g.isConnClosed(err) {
			close(packetChan) // Close the channel when we're done
			return
		}

		// Faster validation using direct indexing when possible
		validated := false
		for _, v := range g.Players {
			if addr.IP.Equal(net.ParseIP(v.IP)) {
				validated = true
				break // Exit early once validated
			}
		}
		if !validated {
			g.Logger.Error(fmt.Errorf("invalid udp connection"), "bad IP", "IP", addr.IP)
			continue
		}

		// Process packet based on mode
		if g.IsRollback {
			// For rollback mode, use the worker pool for better concurrency
			dataCopy := make([]byte, n)
			copy(dataCopy, buf[:n])
			
			// Try to send to channel, but don't block if channel is full
			select {
			case packetChan <- struct {
				addr *net.UDPAddr
				data []byte
			}{addr, dataCopy}:
				// Successfully sent to channel
			default:
				// Channel full, process directly to avoid dropping
				bufCopy := make([]byte, n)
				copy(bufCopy, buf[:n])
				go g.processUDP(addr, bufCopy)
				atomic.AddUint64(&g.PacketsProcessed, 1) // Count this as processed
			}
		} else {
			// For standard mode, process synchronously
			g.processUDP(addr, buf[:n])
		}
	}
}

func (g *GameServer) createUDPServer() error {
	var err error
	g.UDPListener, err = net.ListenUDP("udp", &net.UDPAddr{Port: g.Port})
	if err != nil {
		return err //nolint:wrapcheck
	}
	
	// Set socket buffers to 128KB - a reasonable size for N64 netplay
	// N64 games typically run at 30fps with small input packets (< 20 bytes)
	// This provides enough headroom without excessive memory usage
	if err := g.UDPListener.SetReadBuffer(131072); err != nil {
		g.Logger.Error(err, "could not set UDP read buffer size")
	}
	if err := g.UDPListener.SetWriteBuffer(131072); err != nil {
		g.Logger.Error(err, "could not set UDP write buffer size")
	}
	
	// Improved packet prioritization with AF31 class (DSCP 26) for better gaming traffic QoS
	if err := ipv4.NewConn(g.UDPListener).SetTOS(0x68); err != nil { // AF31 = 26 << 2 = 0x68
		g.Logger.Error(err, "could not set IPv4 DSCP AF31")
	}
	if err := ipv6.NewConn(g.UDPListener).SetTrafficClass(0x68); err != nil { // AF31 = 26 << 2 = 0x68
		g.Logger.Error(err, "could not set IPv6 DSCP AF31")
	}
	
	// No timeouts for UDP sockets to ensure we can handle large batches of packets
	if err := g.UDPListener.SetDeadline(time.Time{}); err != nil {
		g.Logger.Error(err, "could not clear UDP deadline")
	}

	g.Logger.Info("Created UDP server with optimized buffers and AF31 packet priority", "port", g.Port)

	g.GameData.PlayerAddresses = make([]*net.UDPAddr, 4) //nolint:gomnd,mnd

	// Use fixed buffer sizes for rollback mode, standard sizes for regular netplay
	if g.IsRollback {
		g.Logger.Info("Using fixed buffer size for rollback netcode", "bufferSize", RollbackFixedBuffer)
		g.GameData.BufferSize = []uint32{RollbackFixedBuffer, RollbackFixedBuffer, RollbackFixedBuffer, RollbackFixedBuffer}
	} else {
		g.GameData.BufferSize = []uint32{3, 3, 3, 3}
	}
	
	// Initialize buffer health to match targets
	if g.IsRollback {
		g.GameData.BufferHealth = []int32{RollbackBufferTarget, RollbackBufferTarget, RollbackBufferTarget, RollbackBufferTarget}
	} else {
		g.GameData.BufferHealth = []int32{-1, -1, -1, -1}
	}
	
	g.GameData.Inputs = make([]map[uint32]uint32, 4) //nolint:gomnd,mnd
	for i := range 4 {
		g.GameData.Inputs[i] = make(map[uint32]uint32)
	}
	g.GameData.Plugin = make([]map[uint32]byte, 4) //nolint:gomnd,mnd
	for i := range 4 {
		g.GameData.Plugin[i] = make(map[uint32]byte)
	}
	g.GameData.PendingInput = make([]uint32, 4) //nolint:gomnd,mnd
	g.GameData.PendingPlugin = make([]byte, 4)  //nolint:gomnd,mnd
	g.GameData.SyncValues = make(map[uint32][]byte)
	g.GameData.PlayerAlive = make([]bool, 4) //nolint:gomnd,mnd
	g.GameData.CountLag = make([]uint32, 4)  //nolint:gomnd,mnd

	go g.watchUDP()
	return nil
}
