package pebbleds

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	pebbleds "github.com/ipfs/go-ds-pebble"
	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"
	"path/filepath"
)

var Plugins = []plugin.Plugin{
	&pebbledsPlugin{},
}

type pebbledsPlugin struct{}

var _ plugin.PluginDatastore = (*pebbledsPlugin)(nil)

func (*pebbledsPlugin) Name() string {
	return "ds-pebbleds"
}

func (*pebbledsPlugin) Version() string {
	return "0.3.1"
}

func (*pebbledsPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (*pebbledsPlugin) DatastoreTypeName() string {
	return "pebbleds"
}

type datastoreConfig struct {
	path         string
	cacheSize    int64              // default: 8MB, see Options.Cache
	bytesPerSync int                // default: 512 KB, see Options.BytesPerSync
	memTableSize uint64             // default: 64 MB, see Options.MemTableSize
	maxOpenFiles int                // default: 1000, see Options.MaxOpenFiles
	compression  pebble.Compression // default: zstd, see Options.Levels
}

func (*pebbledsPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		var c datastoreConfig
		var ok bool

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not string")
		}

		switch cm := params["compression"]; cm {
		case "zstd", "", nil:
			c.compression = pebble.ZstdCompression
		case "snappy":
			c.compression = pebble.SnappyCompression
		case "default":
			c.compression = pebble.DefaultCompression
		case "none":
			c.compression = pebble.NoCompression
		default:
			return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
		}

		c.cacheSize, ok = params["cache"].(int64)
		if !ok {
			c.cacheSize = 8 * 1024 * 1024
		}
		c.bytesPerSync, ok = params["bytesPerSync"].(int)
		if !ok {
			c.bytesPerSync = 512 * 1024
		}
		c.memTableSize, ok = params["memTableSize"].(uint64)
		if !ok {
			c.memTableSize = 64 * 1024 * 1024
		}
		c.maxOpenFiles, ok = params["maxOpenFiles"].(int)
		if !ok {
			c.maxOpenFiles = 1000
		}
		return &c, nil
	}
}
func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type": "pebbleds",
		"path": c.path,
	}
}
func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}
	return pebbleds.NewDatastore(p, &pebble.Options{
		BytesPerSync: c.bytesPerSync,
		Cache:        pebble.NewCache(c.cacheSize),
		MemTableSize: c.memTableSize,
		MaxOpenFiles: c.maxOpenFiles,
		Levels: []pebble.LevelOptions{
			{Compression: c.compression},
		},
	})
}
