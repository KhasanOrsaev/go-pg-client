package dwh_client_git

import (
	"database/sql"
	"fmt"
	"git.fin-dev.ru/scm/dmp/dwh_client.git/config"
	_ "github.com/lib/pq"
)

type DWH struct {
	Configuration *config.Configuration
	DB *sql.DB
}

func NewClient() *DWH {
	return &DWH{}
}

var err error

func (client *DWH) SetConfig(f []byte) error {
	client.Configuration, err = config.InitConfig(f)
	return err
}

func (client *DWH) OpenConnection() error {
	client.DB, err = sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		client.Configuration.User,client.Configuration.Password,client.Configuration.Host,
		client.Configuration.Database))
	if err != nil {
		return err
	}
	return nil
}

func (client *DWH) CloseConnection() error {
	return client.DB.Close()
}
