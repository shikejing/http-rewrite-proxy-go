package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreReplaceAndCurrent(t *testing.T) {
	s := NewStore("")
	if s.Current() == nil {
		t.Fatal("初始应有默认配置")
	}
	cfg := validConfig()
	if err := s.Replace(cfg); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if s.Current() != cfg {
		t.Fatal("Current 应返回替换后的配置")
	}
	// 非法配置不替换（回滚）。
	bad := validConfig()
	bad.Settings.AttemptTimeoutMS = -1
	if err := s.Replace(bad); err == nil {
		t.Fatal("非法配置应被拒绝")
	}
	if s.Current() != cfg {
		t.Fatal("非法配置不应替换现有快照")
	}
}

func TestStoreLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := validConfig()
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewStore(path)
	if err := s.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.Current().Settings.DefaultUpstream != "default" {
		t.Fatal("加载的配置不正确")
	}
}

func TestStoreLoadMissingFile(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "missing.json"))
	if err := s.Load(); err == nil {
		t.Fatal("加载不存在的文件应报错")
	}
}
