package fakeredis

import (
    "errors"
    "strconv"
    "container/list"
    "time"
    "sync"
)

type FakeRedis struct {
    memory map[string]string
    hmemory map[string]map[string]string
    qmemory map[string]*list.List
    mutex sync.Mutex

}

func New() *FakeRedis {
    var this FakeRedis

    this.memory = make(map[string]string)
    this.hmemory = make(map[string]map[string]string)
    this.qmemory = make(map[string]*list.List)

    return &this
}

func (this *FakeRedis) Hset(key string, field string, value string) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, a new key is created
    if _, ok := this.hmemory[key]; !ok {
        this.hmemory[key] = make(map[string]string)
    }

    this.hmemory[key][field] = value
}

func (this *FakeRedis) Hincrby(key string, field string, amount int) (string, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, a new key is created
    if _, ok := this.hmemory[key]; !ok {
        this.hmemory[key] = make(map[string]string)
    }

    // if value does not exists, set it to 0
    if _, ok := this.hmemory[key][field]; !ok {
        this.hmemory[key][field] = "0"
    }

    value, err := strconv.Atoi(this.hmemory[key][field])
    if (err != nil) {
        return "0", errors.New("Field is not numeric")
    }
    
    retvalue := strconv.Itoa(value+amount)
    this.hmemory[key][field] = retvalue

    return this.hmemory[key][field], nil
}

func (this *FakeRedis) Hget(key string, field string) (string, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    if _, ok := this.hmemory[key]; !ok {
        return "", errors.New("Invalid key")
    }

    if _, ok := this.hmemory[key][field]; !ok {
        return "", errors.New("Invalid field")
    }

    return this.hmemory[key][field], nil
}

func (this *FakeRedis) Hgetall(key string) (map[string]string, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, a new key is created
    if _, ok := this.hmemory[key]; !ok {
        this.hmemory[key] = make(map[string]string)
    }

    return this.hmemory[key], nil
}

func (this *FakeRedis) Hexists(key string, field string) (bool, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    if _, ok := this.hmemory[key]; !ok {
        return false, nil
    }
    if _, ok := this.hmemory[key][field]; !ok {
        return false, nil
    }

    return true, nil
}

func (this *FakeRedis) Set(key string, value string) (error) {
    this.Hset("|s|"+key, "default", value)
    return nil
}

func (this *FakeRedis) Get(key string) (string, error) {
    return this.Hget("|s|"+key, "default")
}

func (this *FakeRedis) Lpush(key string, value string)  (error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, a new key is created
    if _, ok := this.qmemory[key]; !ok {
        this.qmemory[key] = list.New()
    }

    this.qmemory[key].PushFront(value)
    return nil
}

func (this *FakeRedis) Rpush(key string, value string) (error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, a new key is created
    if _, ok := this.qmemory[key]; !ok {
        this.qmemory[key] = list.New()
    }

    this.qmemory[key].PushBack(value)
    return nil
}

func (this *FakeRedis) Lpop(key string)  (string, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, retur nil
    if _, ok := this.qmemory[key]; !ok {
        return "", errors.New("Empty list")
    }

    element := this.qmemory[key].Front()
    this.qmemory[key].Remove(element)

    return element.Value.(string), nil
}

func (this *FakeRedis) Rpop(key string)  (string, error) {
    this.mutex.Lock()
    defer this.mutex.Unlock()

    // if key does not exists, retur nil
    if _, ok := this.qmemory[key]; !ok {
        return "", errors.New("Empty list")
    }

    element := this.qmemory[key].Back()
    this.qmemory[key].Remove(element)

    return element.Value.(string), nil
}

func (this *FakeRedis) Blpop(key string) (string, error) {
    for {
        value, err := this.Lpop(key)
        if err != nil {
            return value, nil
        }
        time.Sleep(1 * time.Second)
    }
}
func (this *FakeRedis) Brpop(key string) (string, error) {
    for {
        value, err := this.Rpop(key)
        if err != nil {
            return value, nil
        }
        time.Sleep(1 * time.Second)
    }
}


