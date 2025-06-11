package gameserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type InputData struct {
	keys   uint32
	plugin byte
}

type GameData struct {
	SyncValues      *lru.Cache[uint32, []byte]
	PlayerAddresses []*net.UDPAddr
	BufferSize      uint32
	BufferHealth    []int32
	Inputs          []*lru.Cache[uint32, InputData]
	PendingInput    []uint32
	CountLag        []uint32
	PendingPlugin   []byte
	sendBuffer      []byte
	recvBuffer      []byte
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
	StatusDesync           = 1
	DisconnectTimeoutS     = 30
	NoRegID                = 255
	InputDataMax       int = 60 * 60 // One minute of input data
	CS4                    = 32
)

// returns true if v is bigger than w (accounting for uint32 wrap around).
func uintLarger(v uint32, w uint32) bool {
	return (w - v) > (math.MaxUint32 / 2)
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

func (g *GameServer) fillInput(playerNumber byte, count uint32) InputData {
	input, inputExists := g.GameData.Inputs[playerNumber].Get(count)
	if !inputExists {
		input = InputData{keys: g.GameData.PendingInput[playerNumber], plugin: g.GameData.PendingPlugin[playerNumber]}
		g.GameData.Inputs[playerNumber].Add(count, input)
	}
	return input
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
	for _, lag := range countLags {
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

func (g *GameServer) sendUDPInput(count uint32, addr *net.UDPAddr, playerNumber byte, spectator bool, sendingPlayerNumber byte) uint32 {
	var countLag uint32
	if uintLarger(count, g.GameData.LeadCount) {
		if !spectator {
			g.Logger.Error(fmt.Errorf("bad count lag"), "count is larger than LeadCount", "count", count, "LeadCount", g.GameData.LeadCount, "playerNumber", playerNumber)
		}
	} else {
		countLag = g.GameData.LeadCount - count
	}
	if sendingPlayerNumber == NoRegID { // if the incoming packet was KeyInfoClient, the regID isn't included in the packet
		g.GameData.sendBuffer[0] = KeyInfoServerGratuitous // client will ignore countLag value in this case
	} else {
		g.GameData.sendBuffer[0] = KeyInfoServer
	}
	g.GameData.sendBuffer[1] = playerNumber
	g.GameData.sendBuffer[2] = g.GameData.Status
	g.GameData.sendBuffer[3] = uint8(countLag)
	currentByte := 5
	start := count
	end := start + g.GameData.BufferSize
	_, ok := g.GameData.Inputs[playerNumber].Get(count) // check if input exists for this count
	for (currentByte < len(g.GameData.sendBuffer)-9) && ((!spectator && countLag == 0 && uintLarger(end, count)) || ok) {
		binary.BigEndian.PutUint32(g.GameData.sendBuffer[currentByte:], count)
		currentByte += 4
		input := g.fillInput(playerNumber, count)
		binary.BigEndian.PutUint32(g.GameData.sendBuffer[currentByte:], input.keys)
		currentByte += 4
		g.GameData.sendBuffer[currentByte] = input.plugin
		currentByte++
		count++
		_, ok = g.GameData.Inputs[playerNumber].Get(count) // check if input exists for this count
	}

	if count > start {
		g.GameData.sendBuffer[4] = uint8(count - start) // number of counts in packet
		_, err := g.UDPListener.WriteToUDP(g.GameData.sendBuffer[0:currentByte], addr)
		if err != nil {
			g.Logger.Error(err, "could not send input")
		}
	}
	return countLag
}

func (g *GameServer) processUDP(addr *net.UDPAddr) {
	playerNumber := g.GameData.recvBuffer[1]
	switch g.GameData.recvBuffer[0] {
	case KeyInfoClient:
		g.GameData.PlayerAddresses[playerNumber] = addr
		count := binary.BigEndian.Uint32(g.GameData.recvBuffer[2:])

		g.GameData.PendingInput[playerNumber] = binary.BigEndian.Uint32(g.GameData.recvBuffer[6:])
		g.GameData.PendingPlugin[playerNumber] = g.GameData.recvBuffer[10]

		for i := range 4 {
			if g.GameData.PlayerAddresses[i] != nil {
				g.sendUDPInput(count, g.GameData.PlayerAddresses[i], playerNumber, true, NoRegID)
			}
		}
	case PlayerInputRequest:
		regID := binary.BigEndian.Uint32(g.GameData.recvBuffer[2:])
		count := binary.BigEndian.Uint32(g.GameData.recvBuffer[6:])
		spectator := g.GameData.recvBuffer[10]
		if uintLarger(count, g.GameData.LeadCount) && spectator == 0 {
			g.GameData.LeadCount = count
		}
		sendingPlayerNumber, err := g.getPlayerNumberByID(regID)
		if err != nil {
			g.Logger.Error(err, "could not process request", "regID", regID)
			return
		}
		countLag := g.sendUDPInput(count, addr, playerNumber, spectator != 0, sendingPlayerNumber)
		g.GameData.BufferHealth[sendingPlayerNumber] = int32(g.GameData.recvBuffer[11])

		g.GameDataMutex.Lock() // PlayerAlive can be modified by ManagePlayers in a different thread
		g.GameData.PlayerAlive[sendingPlayerNumber] = true
		g.GameDataMutex.Unlock()

		g.GameData.CountLag[sendingPlayerNumber] = countLag
	case CP0Info:
		if g.GameData.Status&StatusDesync == 0 {
			viCount := binary.BigEndian.Uint32(g.GameData.recvBuffer[1:])
			syncValue, ok := g.GameData.SyncValues.Get(viCount)
			if !ok {
				g.GameData.SyncValues.Add(viCount, g.GameData.recvBuffer[5:133])
			} else if !bytes.Equal(syncValue, g.GameData.recvBuffer[5:133]) {
				g.GameDataMutex.Lock() // Status can be modified by ManagePlayers in a different thread
				g.GameData.Status |= StatusDesync
				g.GameDataMutex.Unlock()

				g.Logger.Error(fmt.Errorf("desync"), "game has desynced", "numPlayers", g.NumberOfPlayers, "clientSHA", g.ClientSha, "playTime", time.Since(g.StartTime).String(), "features", g.Features)
			}
		}
	}
}

func (g *GameServer) watchUDP() {
	for {
		_, addr, err := g.UDPListener.ReadFromUDP(g.GameData.recvBuffer)
		if err != nil && !g.isConnClosed(err) {
			g.Logger.Error(err, "error from UdpListener")
			continue
		} else if g.isConnClosed(err) {
			return
		}

		validated := false
		for _, v := range g.Players {
			if addr.IP.Equal(v.IP) {
				validated = true
			}
		}
		if !validated {
			g.Logger.Error(fmt.Errorf("invalid udp connection"), "bad IP", "IP", addr.IP)
			continue
		}

		g.processUDP(addr)
	}
}

func (g *GameServer) createUDPServer() error {
	var err error
	g.UDPListener, err = net.ListenUDP("udp", &net.UDPAddr{Port: g.Port})
	if err != nil {
		return err
	}
	if err := ipv4.NewConn(g.UDPListener).SetTOS(CS4 << 2); err != nil {
		g.Logger.Error(err, "could not set IPv4 DSCP")
	}
	if err := ipv6.NewConn(g.UDPListener).SetTrafficClass(CS4 << 2); err != nil {
		g.Logger.Error(err, "could not set IPv6 DSCP")
	}
	g.Logger.Info("Created UDP server", "port", g.Port)

	g.GameData.PlayerAddresses = make([]*net.UDPAddr, 4)
	g.GameData.BufferSize = 3
	g.GameData.BufferHealth = []int32{-1, -1, -1, -1}
	g.GameData.Inputs = make([]*lru.Cache[uint32, InputData], 4)
	for i := range 4 {
		g.GameData.Inputs[i], _ = lru.New[uint32, InputData](InputDataMax)
	}
	g.GameData.PendingInput = make([]uint32, 4)
	g.GameData.PendingPlugin = make([]byte, 4)
	g.GameData.SyncValues, _ = lru.New[uint32, []byte](100) // Store up to 100 sync values
	g.GameData.PlayerAlive = make([]bool, 4)
	g.GameData.CountLag = make([]uint32, 4)
	g.GameData.sendBuffer = make([]byte, 508)
	g.GameData.recvBuffer = make([]byte, 1500)

	go g.watchUDP()
	return nil
}
