package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// ReleaseChannel releases a single channel back to the pool.
// Called when a channel is paused or deleted.
func (c *Coordinator) ReleaseChannel(username, site string) {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	if err := c.Client.ReleaseChannel(username, site); err != nil {
		log.Printf("[coordinator] error releasing channel %s/%s: %v", site, username, err)
	}
}

// CreateChannelAssignment creates a channel_assignments row for a new channel.
// The row is created with status='unassigned' so any node can claim it.
func (c *Coordinator) CreateChannelAssignment(conf *entity.ChannelConfig) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	ca := database.ChannelAssignment{
		Username:                conf.Username,
		Site:                    conf.Site,
		Status:                  "unassigned",
		IsLive:                  false,
		Framerate:               conf.Framerate,
		Resolution:              conf.Resolution,
		Pattern:                 conf.Pattern,
		MaxDuration:             conf.MaxDuration,
		MaxFilesize:             conf.MaxFilesize,
		Compress:                conf.Compress,
		MinDurationBeforeUpload: conf.MinDurationBeforeUpload,
	}

	if err := c.Client.BulkInsertAssignments([]database.ChannelAssignment{ca}); err != nil {
		return err
	}

	// IMPORTANT: do NOT claim the new channel to THIS node. The row is created
	// with status='unassigned', and the leader-elected controller's balanceSite
	// sweep claims every unassigned channel into the fleet-wide equal split on
	// its very next cycle. Claiming here would pin the channel to whichever node
	// happened to add it — and if it goes live and records before the controller
	// rebalances, it would be stuck there, breaking the even distribution.
	log.Printf("[coordinator] created channel %s/%s (unassigned) — controller will assign it", conf.Site, conf.Username)

	return nil
}

// DeleteChannelAssignment removes the channel_assignments row for a channel.
func (c *Coordinator) DeleteChannelAssignment(username, site string) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	return c.Client.ReleaseChannel(username, site)
}

// ConfigFromAssignment converts a ChannelAssignment back to a ChannelConfig.
func ConfigFromAssignment(ca *database.ChannelAssignment) *entity.ChannelConfig {
	conf := &entity.ChannelConfig{
		Site:                    ca.Site,
		Username:                ca.Username,
		Framerate:               ca.Framerate,
		Resolution:              ca.Resolution,
		Pattern:                 ca.Pattern,
		MaxDuration:             ca.MaxDuration,
		MaxFilesize:             ca.MaxFilesize,
		Compress:                ca.Compress,
		MinDurationBeforeUpload: ca.MinDurationBeforeUpload,
		CreatedAt:               time.Now().Unix(),
	}
	conf.Sanitize()
	return conf
}

// MarshalPool marshals a slice of ChannelConfig into JSON bytes.
func MarshalPool(pool []*entity.ChannelConfig) ([]byte, error) {
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return json.MarshalIndent(pool, "", "  ")
}

// UnmarshalPool unmarshals JSON bytes into a slice of ChannelConfig.
func UnmarshalPool(data []byte) ([]*entity.ChannelConfig, error) {
	var pool []*entity.ChannelConfig
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, err
	}
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return pool, nil
}
