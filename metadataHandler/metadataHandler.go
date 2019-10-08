//Package that handles reading and writing into metadata file
package metadataHandler

import (
    "github.com/renjithwarrier94/disk_store/logger"
    "os"
    "fmt"
    "syscall"
    "encoding/gob"
    "bytes"
    "errors"
)

const metdataIntervalLength = 150

type Metadata struct {
    data            []byte
    file            *os.File
    currentSlot     *Slot
    currentSlotNo   uint64
    log             *logger.Logger
}

//Get the metadata type for the metadata file mapped to a byte slice
func GetMetadata(path string, fileSize int64) (*Metadata, error) {
    metadata := Metadata{log: logger.GetLogger(true)}
    var fileUseSize int64
    f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
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
        fileUseSize = fileSize
    } else {
        fileUseSize = info.Size()
    }

    //Get memory mapped byte slice
    d, err := syscall.Mmap(int(f.Fd()), 0, int(fileUseSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        metadata.log.Errorf(fmt.Sprintf("Error %v when creating mmap for metadata file", err))
        return nil, err
    }
    //Add data to metadata object
    metadata.data = d
    return &metadata, nil
}

func (m Metadata) WriteSlot(s Slot, slotNo uint64) error {
    writeOffset := slotNo * metdataIntervalLength
    writeSlice := m.data[writeOffset:writeOffset+metdataIntervalLength]
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
    readSlice := m.data[readOffset: readOffset+metdataIntervalLength]
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
