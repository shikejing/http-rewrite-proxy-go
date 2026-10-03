package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
)

// Store 持有原子配置快照，支持热更新与失败回滚。
type Store struct {
	path    string
	current atomic.Pointer[Config]
}

// NewStore 创建存储。
func NewStore(path string) *Store {
	s := &Store{path: path}
	s.current.Store(Default())
	return s
}

// Current 返回当前配置快照。
func (s *Store) Current() *Config {
	return s.current.Load()
}

// Load 从文件重新加载；校验失败时保留旧快照（回滚）。
func (s *Store) Load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	return s.Replace(&cfg)
}

// Replace 校验并替换配置快照；失败时保留旧快照（回滚）。
func (s *Store) Replace(cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	s.current.Store(cfg)
	return nil
}
