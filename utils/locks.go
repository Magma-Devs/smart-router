package utils

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const TIMEOUT = 10

var (
	TimeoutMutex           = "false"
	TimeoutMutexBoolean, _ = strconv.ParseBool(TimeoutMutex)
)

type Lockable interface {
	Lock()
	TryLock() bool
	Unlock()
}

type Mutex struct {
	mu          sync.Mutex
	quit        chan bool
	SecondsLeft int
	lineAndFile string
	lockCount   int
}

func (dm *Mutex) getLineAndFile() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", file, line)
}

func (dm *Mutex) waitForTimeout() {
	dm.quit = make(chan bool)
	ticker := time.NewTicker(TIMEOUT * time.Second)
	go func() {
		for {
			select {
			case <-dm.quit:
				ticker.Stop()
				return
			case <-ticker.C:
				ticker.Stop()
				fmt.Printf("WARNING: Mutex is Locked for more than %d seconds \n %s \n", TIMEOUT, dm.lineAndFile)
				return
			}
		}
	}()
}

func (dm *Mutex) Lock() {
	if TimeoutMutexBoolean {
		tempLineAndFile := dm.getLineAndFile()
		dm.lockCount++
		fmt.Printf("Lock: %s, count %d ... ", tempLineAndFile, dm.lockCount)
		dm.mu.Lock()
		fmt.Printf("locked \n")
		dm.lineAndFile = tempLineAndFile
		dm.SecondsLeft = TIMEOUT
		dm.waitForTimeout()
	} else {
		dm.mu.Lock()
	}
}

func (dm *Mutex) TryLock() (isLocked bool) {
	if TimeoutMutexBoolean {
		tempLineAndFile := dm.getLineAndFile()
		isLocked = dm.mu.TryLock()
		if isLocked {
			dm.lockCount++
			// fmt.Println("TryLock Locked: ", tempLineAndFile)
			dm.lineAndFile = tempLineAndFile
			dm.SecondsLeft = TIMEOUT
			dm.waitForTimeout()
		}
		return isLocked
	} else {
		return dm.mu.TryLock()
	}
}

func (dm *Mutex) Unlock() {
	if TimeoutMutexBoolean {
		// fmt.Println("Unlock: ", dm.getLineAndFile())
		dm.lockCount++
		dm.quit <- true
	}
	dm.mu.Unlock()
}

// func main() {
// 	x := Mutex{}
// 	x.Lock()

// 	time.Sleep(6 * time.Second)

// 	x.Unlock()

// 	return
// }
