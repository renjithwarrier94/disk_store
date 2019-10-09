//This package deals with writing, reading, updating, deleting and providing related operations on the metadata file.
//Each record in this file is of fixed length and is uniquely identified by its slot number
package metadataHandler

import (
    "github.com/renjithwarrier94/disk_store/logger"
    "os"
    "fmt"
    "syscall"
    "encoding/gob"
    "bytes"
    "errors"
    "math"
)

//The interval to leave for each slot data
const metdataIntervalLength = 200
//The name of the metadata file. Its stored in the given path + fileName
const metadataFileName = "metadata.ds"
//All the status byte flags
const (
    SLOT_IN_USE                 = byte(0x01)
    SLOT_BEING_MODIFIED         = byte(0x02)
    SLOT_MARKED_FOR_DELETION    = byte(0x04)
)

type Metadata struct {
    data            []byte
    file            *os.File
    currentSlot     *Slot
    currentSlotNo   uint64
    log             *logger.Logger
}

//Get the metadata type for the metadata file mapped to a byte slice
//Pass the path where the data files need to live/exists and the size in bytes
//If the size is less than 4096 bytes (4 KB), 4096 is used
//If it is more than 4096, the nearest multiple of 4096 ie ceil(fileSize/4096)*4096 is used
func GetMetadata(path string, fileSize int64) (*Metadata, error) {
    metadata := Metadata{log: logger.GetLogger(true)}
    fileSize = int64(math.Ceil(float64(fileSize)/4096.0) * 4096)
    f, err := os.OpenFile(path + metadataFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
    if err != nil {
        metadata.log.Errorf(fmt.Sprintf("Error %v when trying to open metadata file", err))
        return nil, err
    }
    //Add File type to metadata
    metadata.file = f
    info, err := f.Stat()
    if err != nil {
        metadata.log.Errorf(fmt.Sprintf("Error %v when trying to get metadata file stats", err))
        return nil, err
    }
    if info.Size() < fileSize {
        err = f.Truncate(fileSize)
        if err != nil {
            metadata.log.Errorf(fmt.Sprintf("Error %v when trying to truncate metadata file", err))
            return nil, err
        }
    } else {
        fileSize = info.Size()
    }

    //Get memory mapped byte slice
    d, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        metadata.log.Errorf(fmt.Sprintf("Error %v when creating mmap for metadata file", err))
        return nil, err
    }
    //Add data to metadata object
    metadata.data = d
    return &metadata, nil
}

//The slot is written at the location slotNo*metdataIntervalLength
//The first byte is the status byte. It is the ORed value of the SLOT_* constants (whichever applicable)
//Rest of it is the slot data
func (m Metadata) WriteSlot(s Slot, slotNo uint64) error {
    writeOffset := slotNo * metdataIntervalLength
    //TODO: MAKE THE OPERATION ATOMIC
    //Set the status byte to show the slot is in use and is being modified
    statusByte := SLOT_IN_USE | SLOT_BEING_MODIFIED
    m.data[writeOffset] = statusByte

    writeSlice := m.data[writeOffset+1:writeOffset+metdataIntervalLength]
    var buffer bytes.Buffer
    encoder := gob.NewEncoder(&buffer)
    err := encoder.Encode(s)
    if err != nil {
        m.log.Errorf(fmt.Sprintf("Error %v when encoding slot", err))
        return err
    }
    readSlice := buffer.Bytes()
    n := copy(writeSlice, readSlice)
    if tot := len(readSlice); n != tot {
        m.log.Errorf(fmt.Sprintf("Could not write the entire metadata. Could only write %v of %v bytes", n, tot))
        return errors.New(fmt.Sprintf("Could not write the entire metadata. Could only write %v of %v bytes", n, tot))
    }

    //Set the status byte to show that the slot is in use only
    statusByte = SLOT_IN_USE
    m.data[writeOffset] = statusByte
    return nil
}

func (m Metadata) CloseFile() error {
    err := m.file.Sync()
    if err != nil {
        m.log.Errorf(fmt.Sprintf("Error %v when syncing metadata file", err))
        return err
    }
    err = m.file.Close()
    if err != nil {
        m.log.Errorf(fmt.Sprintf("Error %v when closing metadata file", err))
        return err
    }
    return nil
}

func (m Metadata) GetSlot(slotno uint64) (Slot, error) {
    readOffset := slotno * metdataIntervalLength
    //1st Byte is the status byte. Read from the next byte for slot data
    readSlice := m.data[readOffset+1: readOffset+metdataIntervalLength]
    var decodedSlot Slot
    buffer := bytes.NewBuffer(readSlice)
    decoder := gob.NewDecoder(buffer)
    err := decoder.Decode(&decodedSlot)
    if err != nil {
        m.log.Errorf(fmt.Sprintf("Error %v when decoding slot", err))
        return decodedSlot, err
    }
    return decodedSlot, nil
}
