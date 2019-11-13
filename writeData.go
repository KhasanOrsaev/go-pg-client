package dwh_client_git

import (
	"encoding/json"
	"errors"
	"github.com/lib/pq"
	"sync"
)

type Request struct {
	UserID string `json:"user_id" db:"user_id"`
	Type string `json:"type" db:"type"`
	Source string `json:"source" db:"source"`
	Data *json.RawMessage `json:"data" db:"data"`
}

func (client *DWH) WriteData(outChannel <- chan map[interface{}][]byte, confirmChannel chan <- interface{},
	crashChannel chan <- []byte, errChannel chan <- error) {
	ws := sync.WaitGroup{}
	for i:= 0; i < client.Configuration.Workers; i++ {
		ws.Add(1)
		go func(outChannel <- chan map[interface{}][]byte, confirmChannel chan <- interface{}, crashChannel chan <- []byte, group *sync.WaitGroup) {
			defer group.Done()
			requests := make([]Request, 0, client.Configuration.Bulk)
			for d := range outChannel {
				r := Request{}
				// если достигнуто предельное значение в пачке, то пишем в базу
				if len(requests) == client.Configuration.Bulk {
					client.sendToDB(requests, crashChannel, errChannel)
					requests = make([]Request, 0, client.Configuration.Bulk)
				}
				for i,v := range d {
					// данные из очереди, json декодировка
					err := json.Unmarshal(v, &r)
					if err!=nil {
						confirmChannel <- i
						crashChannel <- v
						errChannel <- err
						continue
					}

					// validation
					if r.UserID == "" || r.Type == "" || r.Source == "" {
						confirmChannel <- i
						crashChannel <- v
						errChannel <- errors.New("user_id or type or source is empty")
						continue
					}

					requests = append(requests, r)

					confirmChannel <- i
				}
			}
			// если по завершению цикла в пачке есть записи, то пишем в базу
			if len(requests) > 0  {
				client.sendToDB(requests, crashChannel, errChannel)
			}
		}(outChannel, confirmChannel, crashChannel, &ws)
	}
	ws.Wait()
}

// если ошибка записи то отправляем всю пачку в crash
func toCrashChannel(requests []Request, crashChannel chan <- []byte) {
	for request := range requests {
		r,_ := json.Marshal(request)
		crashChannel <- r
	}
}

func (client *DWH) sendToDB(requests []Request, crashChannel chan <- []byte, errChannel chan <- error)  {
	txn, err := client.DB.Begin()
	if err != nil {
		errChannel <- err
		toCrashChannel(requests, crashChannel)
		return
	}
	stmt, err := txn.Prepare(pq.CopyInSchema("cs_events","events", "user_id", "type", "source", "data"))
	if err != nil {
		errChannel <- err
		toCrashChannel(requests, crashChannel)
		return
	}
	for _, request := range requests {
		_, err = stmt.Exec(request.UserID, request.Type, request.Source, request.Data)
		if err != nil {
			errChannel <- err
			toCrashChannel(requests, crashChannel)
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		errChannel <- err
		toCrashChannel(requests, crashChannel)
		return
	}
	err = stmt.Close()
	if err != nil {
		errChannel <- err
		toCrashChannel(requests, crashChannel)
		return
	}

	err = txn.Commit()
	if err != nil {
		errChannel <- err
		toCrashChannel(requests, crashChannel)
		return
	}
}