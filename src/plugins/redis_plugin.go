package plugins

import (
	"choose/src/core"
	"github.com/go-redis/redis"
	"log"
	"strconv"
	"time"
)

type RedisVoterImpl struct {
	uuid string

	voterConf *core.VoterTimeConfig //选举需要使用到的超时配置
	cli       *redis.Client
}

func NewRedisVoterImpl(host string, password string, db int) (core.Voter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       db,
	})
	tmp := &RedisVoterImpl{
		cli: client,
	}
	return tmp, nil
}

func (r RedisVoterImpl) Heartbeat() (bool, int64) {
	now := time.Now().UnixMilli()
	err := r.cli.Set(r.uuid, now, 0).Err()
	if err != nil {
		log.Printf("update heartbeat time is error: %s", err.Error())
		return false, 0
	}
	return true, now
}

func (r RedisVoterImpl) GetMasterInfo() (*core.NodeStatus, error) {
	curMaster, err := r.cli.Get("master").Result()
	if err != nil {
		return nil, err
	}
	updateTime, err := r.cli.Get(curMaster).Result()
	updateTime1, _ := strconv.Atoi(updateTime)
	return &core.NodeStatus{
		Uuid:                curMaster,
		LatestHeartbeatTime: int64(updateTime1),
	}, nil
}

func (r RedisVoterImpl) ElectMaster(oldMaster string) (*core.NodeStatus, error) {
	curMaster, err := r.cli.Get("master").Result()
	if err != nil {
		return nil, err
	}
	//主没有更新
	if curMaster == oldMaster {
		err = r.cli.Set("master", r.uuid, 0).Err()
		if err != nil {
			return nil, err
		}
		//TODO 如果只成功了一半？？？
		now := time.Now().UnixMilli()
		err = r.cli.Set(r.uuid, now, 0).Err()
		if err != nil {
			return nil, err
		}
		return &core.NodeStatus{
			Uuid:                r.uuid,
			LatestHeartbeatTime: now,
		}, nil
	}
	return r.GetMasterInfo()
}

func (r RedisVoterImpl) GetUuid() string {
	return r.uuid
}

func (r RedisVoterImpl) GetVoterTimeConf() *core.VoterTimeConfig {
	return r.voterConf
}
