package main

import "github.com/renjithwarrier94/disk_store/logger"

func main() {
    log := logger.GetLogger(true)
    //Raising an Info
    log.Infof("This is info message")
    log.Warnf("This is a warn mesage")
    log.Debugf("This is a debug message")
    log.Errorf("This is an error message")
}
