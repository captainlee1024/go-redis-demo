// Package main provides ...
package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// 声明一个全局的rdb变量
var rdb *redis.Client

// 初始化连接 初始化rdb全局变量
func initClient() (err error) {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "644315",
		DB:       0,   // user default DB
		PoolSize: 100, // 连接池大小
	})

	_, err = rdb.Ping().Result()
	return err
}

// set/get示例
func redisExample() {
	err := rdb.Set("counter3", 0, 0).Err()
	if err != nil {
		fmt.Printf("set score failed, err:%v\n", err)
		return
	}

	var val string
	// 这里使用Val()只返回取到的值，不返回错误，取不到会返回该值类型对应的零值
	// 所以这里使用Result()返回error进行进一步判断
	val, err = rdb.Get("score").Result()
	if err != nil {
		fmt.Printf("get score failed, err:%d\n", err)
		return
	}
	fmt.Println("score:", val)

	val, err = rdb.Get("score2").Result()
	if err == redis.Nil { // redis.Nil 也是一种错误，是所查找的key不存在，应当先判断
		fmt.Printf("score2 doer not exist\n")
	} else if err != nil {
		fmt.Printf("get score2 failed, err:%v\n", err)
		return
	} else {
		fmt.Println("score2:", val)
	}
}

// zset示例
// 大多应用于排行榜、差集交集的集合
func redisExample2() {
	zsetKey := "language_rank"
	languages := []redis.Z{
		redis.Z{Score: 90.0, Member: "Golang"},
		redis.Z{Score: 98.0, Member: "Java"},
		redis.Z{Score: 95.0, Member: "Python"},
		redis.Z{Score: 97.0, Member: "JavaScript"},
		redis.Z{Score: 99.0, Member: "C/C++"},
		redis.Z{Score: 100, Member: "Rust"},
	}

	// ZADD 添加元素
	num, err := rdb.ZAdd(zsetKey, languages...).Result()
	if err != nil {
		fmt.Printf("zadd failed, err:%v\n", err)
		return
	}
	fmt.Printf("zadd %d success.\n", num)

	// 把Golang的分数加10
	var newScore float64
	newScore, err = rdb.ZIncrBy(zsetKey, 10.0, "Golang").Result()
	if err != nil {
		fmt.Printf("zinrcby failed, err:%v\n", err)
		return
	}
	fmt.Printf("Golang's score is %f now.\n", newScore)

	// 取分数最高的3个
	ret, err := rdb.ZRevRangeWithScores(zsetKey, 0, 2).Result()
	if err != nil {
		fmt.Printf("zrevrange failed, err:%v\n", err)
		return
	}
	for _, z := range ret {
		fmt.Println(z.Member, z.Score)
	}

	// 取97~100分的
	op := redis.ZRangeBy{
		Min: "97",
		Max: "100",
	}
	ret, err = rdb.ZRangeByScoreWithScores(zsetKey, op).Result()
	if err != nil {
		fmt.Printf("zrangebyscore failed, err:%v\n", err)
		return
	}
	for _, z := range ret {
		fmt.Println(z.Member, z.Score)
	}
}

// hash示例
func redisExample3() {

	// 给user:1:info添加name和age键值对
	err := rdb.HSet("user:2:info", "姓名", "沙河小王子").Err()
	if err != nil {
		fmt.Printf("hset failed, err:%v\n", err)
		return
	}
	rdb.HSet("user:2:info", "年龄", 20)
	if err != nil {
		fmt.Printf("hset failed, err:%v\n", err)
		return
	}

	// 获取所有字段
	val1, err := rdb.HGetAll("user:2:info").Result()
	// 返回的错误有以下几种
	// 1.redis.Nil
	// 2.其他错误
	if err != nil {
		fmt.Printf("hgetall failed, err:%v\n", err)
		return
	}
	fmt.Printf("user:2:info:%#v\n", val1)

	// 获取指定字段
	val2 := rdb.HMGet("user:2:info", "姓名", "年龄").Val()
	fmt.Printf(":%#v\n", val2)

	// 获取一个字段
	val3 := rdb.HGet("user:2:info", "年龄").Val()
	fmt.Println(val3)
}

// pipeline示例
func pipelineExample() {
	pipe := rdb.Pipeline()
	err := pipe.Set("score3", 100, 0).Err()
	if err != nil {
		fmt.Printf("set score3 failed, err:%v\n", err)
		return
	}

	err = pipe.Set("score4", 200, 0).Err()
	if err != nil {
		fmt.Printf("set score4 failed, err:%v\n", err)
	}

	_, err = pipe.Exec()
	val := rdb.Get("score3")
	fmt.Println(val.Val())
}

// Pipelined 作用同pipeline
func pipelinedExample() {
	_, err := rdb.Pipelined(func(pipe redis.Pipeliner) error {
		pipe.Set("pipelined1", 10, 0)
		pipe.Set("pipelined2", 20, 0)
		return nil
	})
	if err != nil {
		fmt.Printf("pipelined failed, err:%v\n", err)
		return
	}
	fmt.Println("pipelined success...")
}

// Transaction 事务
// Multi/Exec 两个语句之间的命令以及顺序是固定的，不会有其他的命令插进来
// 在 go-redis 中使用　TxPipeline ，总体上与 Pipeline 类似
// 但是它内部会使用 Multi/Exec 包裹排队命令
func transactionExample() {
	txPipe := rdb.TxPipeline()

	incr := txPipe.Incr("tx_pipeline_counter")
	txPipe.Expire("tx_pipeline_counter", time.Hour)

	_, err := txPipe.Exec()
	if err != nil {
		fmt.Printf("exec failed, err:%v\n", err)
	}
	fmt.Println(incr.Val())
}

// Transaction 事务的另一种写法
func transactionExample2() {
	var incr *redis.IntCmd
	_, err := rdb.TxPipelined(func(pipe redis.Pipeliner) error {
		incr = pipe.Incr("tx_pipelined_counter")
		pipe.Expire("tx_pipelined_counter", time.Hour)
		return nil
	})
	if err != nil {
		fmt.Printf("txpipelined failed, err:%v\n", err)
		return
	}
	fmt.Println(incr.Val())
}

// Watch
// 在事务进行期间，见识某个键是否发生了变化（替换、更新、删除等）
// 如果有其他用户抢先对被监视的键进行了一下操作
// 在事务进行Exec的时候,事务将失败，并返回一个错误
// 用户可以根据也无需求进行重试或者放弃
func watchExample() {
	const routineCount = 100

	// key 值增加函数
	increment := func(key string) error {

		// transaction 获取 key 值，并使 value　加1从新存入到 redis 中
		txf := func(tx *redis.Tx) error {
			// 获取当前的值或者零值
			n, err := tx.Get(key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			// key 对应的 value 加1 （乐观锁中的本地操作）
			n++

			// 监视 key 值，在 reids 库中 value 不变的情况下把 n 存入 redis　中作为新的 key 值
			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.Set(key, n, 0)
				return nil
			})
			return err
		}

		// 循环依次增加 key 值
		for retries := routineCount; retries > 0; retries-- {
			// 传入应用 transactino 的key 值增加函数和监听的 key
			err := rdb.Watch(txf, key)
			// 如果事务提交的时候发现key的值发生变化，则事务返回错误，提交失败，如果没有继续增加
			if err != redis.TxFailedErr {
				return err
			}
			// 乐观锁丢失
		}
		return errors.New("increment reached maximum number of retries")
	}

	// 乐观锁
	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment err:", err)
			}
		}()
	}
	wg.Wait()

	n, err := rdb.Get("counter3").Int()
	fmt.Println("ended with ", n, err)
}

func main() {
	if err := initClient(); err != nil {
		fmt.Printf("init redis client failed, err:%v\n", err)
		return
	}
	fmt.Printf("connect redis success...\n")
	// 程序退出时释放相关资源
	defer rdb.Close()

	//redisExample()
	//redisExample2()
	//redisExample3()
	//pipelineExample()
	//pipelinedExample()
	//transactionExample()
	//transactionExample2()
	//watchExample()
}
