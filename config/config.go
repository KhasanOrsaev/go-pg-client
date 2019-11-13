package config

import (
	"github.com/go-errors/errors"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Host string `yaml:"host"`
	Database string `yaml:"database"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	Queries []struct{
		Query string `yaml:"query"`
		PrepareInsert string `yaml:"prepare_statement"`
	}
	Workers int `yaml:"workers"`
	Bulk int `yaml:"bulk"`
}

func InitConfig(f []byte) (*Configuration,error) {
	var conf Configuration
	err := yaml.Unmarshal(f, &conf)
	if err != nil {
		return nil, errors.Wrap(err, -1)
	}
	return &conf, nil
}
